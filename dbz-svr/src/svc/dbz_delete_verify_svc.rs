//! # 删除审核口令校验
//!
//! 删除画布节点前做二次确认，校验的是**当前登录用户的高危操作口令**。审核口令
//! （`adm_user.audit_pswd`）为空时，adm-rs 会回退校验登录口令；是否回退只由 adm-rs
//! 按库内数据判定，dbz 不参与，只把响应里的 `extra.pswdKind` 透传给前端做提示。
//!
//! dbz 不自己保管口令，也不自建 adm 地址，而是复用 api_client 的既有配置：
//!
//! ```text
//! [api.adm]
//! type = "MicroSvc"
//! svc-name = "adm-svr"
//! ```
//!
//! 调用 `adm_api_client::AdmApiClient::auth.verify_audit_password`，把网关透传的
//! `Authorization` 原样转发给 adm-rs；请求体里不接受账号，也不带 `X-User-Id`
//! ——adm-rs 明确以令牌里的用户为准。adm-rs 只做口令核对，**不累加登录失败
//! 次数、不锁号**。
//!
//! 失败按 business code 透传，前端只按码分流：
//! - `ADM00007`：口令不正确（adm-rs 已按实际类型区分审核/登录口令）；
//! - `ADM00008`：校验过于频繁，需要等待窗口结束；
//! - `ADM00009`：账号不可用（停用 / 锁定 / 过期），换口令也没用；
//! - `ADM00005` / HTTP 401：登录状态已失效；
//! - `DBZ00001`：认证服务不可用（网络、超时、5xx、未配置 `[api.adm]`）。
//!
//! `ADM00010`（审核口令未设置）已废弃，adm-rs 不再返回，这里也不再映射。
//!
//! 没有单独的关闭开关：adm-rs 是删除动作的前置依赖，未配置时删除会被拒绝，
//! 不会退回「配置口令」或「不校验直接删」的方案。

use crate::vo::DbzDeletePswdKind;
use adm_api_client::dto::AdmVerifyAuditPasswordDto;
use adm_api_client::get_adm_api_client;
use adm_api_client::vo::{AdmVerifyAuditPasswordVo, AdmVerifyPswdKind};
use robotech::api::Ro;
use robotech::api_client::ApiClientError;

/// 认证服务不可用：dbz 自定义业务码（adm 侧无对应编码）
pub const CODE_VERIFY_UNAVAILABLE: &str = "DBZ00001";

/// adm-rs 口令二次确认失败码
const CODE_WRONG_PASSWORD: &str = "ADM00007";
const CODE_RATE_LIMITED: &str = "ADM00008";
const CODE_USER_UNAVAILABLE: &str = "ADM00009";
const CODE_SESSION_INVALID: &str = "ADM00005";

/// # 口令校验失败
///
/// 业务码原样透传给前端，`msg` 是可以直接展示的文案。
#[derive(Debug, Clone)]
pub struct DbzDeleteVerifyError {
    /// 业务码：`ADM00005` / `ADM00007` / `ADM00008` / `ADM00009` / `DBZ00001`
    pub code: String,
    /// 给用户看的提示
    pub msg: String,
}

impl DbzDeleteVerifyError {
    fn new(code: &str, msg: impl Into<String>) -> Self {
        Self {
            code: code.to_string(),
            msg: msg.into(),
        }
    }
}

/// # 删除审核口令校验服务
pub struct DbzDeleteVerifySvc;

impl DbzDeleteVerifySvc {
    /// # 校验当前登录用户的高危操作口令
    ///
    /// * `authorization` - 原样转发给 adm-rs 的认证头（网关透传）；缺失即视为未登录
    /// * `audit_pswd` - 前端算好的 64 位 SM3 摘要
    ///
    /// ## 返回值
    /// * `Ok(DbzDeletePswdKind)` - 校验通过，附带本次实际校验的口令类型
    /// * `Err(DbzDeleteVerifyError)` - 失败，调用方不得继续删除
    pub async fn verify(
        authorization: Option<&str>,
        audit_pswd: &str,
    ) -> Result<DbzDeletePswdKind, DbzDeleteVerifyError> {
        Self::verify_for(authorization, audit_pswd, "删除").await
    }

    /// # 校验口令（可指定动作名）
    ///
    /// 与 [`verify`](Self::verify) 完全相同，只把认证服务不可用时的「删除已被拒绝」
    /// 换成调用方的动作名（例如「同步」），避免在别的写操作里说成删除。
    pub async fn verify_for(
        authorization: Option<&str>,
        audit_pswd: &str,
        action: &str,
    ) -> Result<DbzDeletePswdKind, DbzDeleteVerifyError> {
        let authorization = authorization
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let Some(authorization) = authorization else {
            return Err(DbzDeleteVerifyError::new(
                CODE_SESSION_INVALID,
                "登录状态已失效：未获取到登录令牌，请重新登录后重试",
            ));
        };

        let audit_pswd = audit_pswd.trim();
        if audit_pswd.is_empty() {
            return Err(DbzDeleteVerifyError::new(
                CODE_WRONG_PASSWORD,
                "口令不能为空",
            ));
        }
        if audit_pswd.len() != 64 || !audit_pswd.chars().all(|ch| ch.is_ascii_hexdigit()) {
            return Err(DbzDeleteVerifyError::new(
                CODE_WRONG_PASSWORD,
                "口令摘要格式不正确，应为 64 位十六进制",
            ));
        }

        let client = get_adm_api_client().map_err(|error| {
            DbzDeleteVerifyError::new(
                CODE_VERIFY_UNAVAILABLE,
                format!("认证服务不可用：未配置 [api.adm] 或客户端未初始化（{error}），{action}已被拒绝"),
            )
        })?;

        let dto = AdmVerifyAuditPasswordDto {
            audit_pswd: audit_pswd.to_string(),
        };
        match client.auth.verify_audit_password(authorization, &dto).await {
            Ok(response) => Self::check_response(response, "200"),
            // 非 2xx 也走框架信封：401 是会话失效，其它按认证服务不可用
            Err(ApiClientError::NonSuccessStatus(_, status, body)) => {
                if status.starts_with("401") {
                    return Err(DbzDeleteVerifyError::new(
                        CODE_SESSION_INVALID,
                        "登录状态已失效：登录令牌已失效，请重新登录",
                    ));
                }
                if let Ok(response) = serde_json::from_str::<Ro<AdmVerifyAuditPasswordVo>>(&body) {
                    return Self::check_response(response, &status);
                }
                Err(DbzDeleteVerifyError::new(
                    CODE_VERIFY_UNAVAILABLE,
                    format!(
                        "认证服务不可用：认证服务返回异常响应 HTTP {status}，{action}已被拒绝"
                    ),
                ))
            }
            // 网络错误、超时、连接被拒：一律按「认证服务不可用」，不能误报成口令错
            Err(error) => Err(DbzDeleteVerifyError::new(
                CODE_VERIFY_UNAVAILABLE,
                format!("认证服务不可用：无法访问认证服务（{error}），{action}已被拒绝"),
            )),
        }
    }

    /// 按 adm-rs 的业务码透传；文案优先用 adm-rs 返回的那句
    fn check_response(
        response: Ro<AdmVerifyAuditPasswordVo>,
        status: &str,
    ) -> Result<DbzDeletePswdKind, DbzDeleteVerifyError> {
        if response.is_ok() {
            // 成功时 adm-rs 必然带 pswdKind；缺失时按 audit 处理，避免误报「已按登录口令校验」
            let kind = response
                .extra
                .map(|vo| vo.pswd_kind)
                .unwrap_or(AdmVerifyPswdKind::Audit);
            return Ok(match kind {
                AdmVerifyPswdKind::Audit => DbzDeletePswdKind::Audit,
                AdmVerifyPswdKind::Login => DbzDeletePswdKind::Login,
            });
        }

        match response.code.as_deref() {
            Some(CODE_WRONG_PASSWORD) => Err(DbzDeleteVerifyError::new(
                CODE_WRONG_PASSWORD,
                Self::non_empty_msg(&response.msg, "口令不正确"),
            )),
            Some(CODE_RATE_LIMITED) => Err(DbzDeleteVerifyError::new(
                CODE_RATE_LIMITED,
                Self::non_empty_msg(
                    &response.msg,
                    "口令校验失败次数过多，请稍后再试",
                ),
            )),
            Some(CODE_USER_UNAVAILABLE) => Err(DbzDeleteVerifyError::new(
                CODE_USER_UNAVAILABLE,
                Self::account_unavailable_msg(&response.msg),
            )),
            Some(CODE_SESSION_INVALID) => Err(DbzDeleteVerifyError::new(
                CODE_SESSION_INVALID,
                "登录状态已失效：登录令牌已失效，请重新登录",
            )),
            _ => {
                let detail = response.msg.trim();
                let detail = if detail.is_empty() {
                    String::new()
                } else {
                    format!("（{detail}）")
                };
                Err(DbzDeleteVerifyError::new(
                    CODE_VERIFY_UNAVAILABLE,
                    format!(
                        "认证服务不可用：认证服务返回异常响应 HTTP {status}{detail}，删除已被拒绝"
                    ),
                ))
            }
        }
    }

    /// adm-rs 的文案一般以「当前用户不可用」开头，统一收敛成「账号不可用」，
    /// 不再伪装成会话失效
    fn account_unavailable_msg(msg: &str) -> String {
        let detail = msg
            .trim()
            .trim_start_matches("当前用户不可用")
            .trim_start_matches(|ch: char| {
                ch == '：' || ch == ':' || ch == '，' || ch == ',' || ch.is_whitespace()
            });
        if detail.is_empty() {
            "账号不可用，请重新登录或联系管理员".to_string()
        } else {
            format!("账号不可用：{detail}")
        }
    }

    fn non_empty_msg(msg: &str, fallback: &str) -> String {
        let msg = msg.trim();
        if msg.is_empty() {
            fallback.to_string()
        } else {
            msg.to_string()
        }
    }
}
