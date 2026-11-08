//! # 删除审核口令校验
//!
//! 删除画布节点前做二次确认，校验的是**当前登录用户的审核口令**（adm 用户表里
//! 独立的 `audit_pswd`），不是登录口令。dbz 不自己保管口令，也不自建 adm 地址，
//! 而是复用 api_client 的既有配置：
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
//! 失败必须能区分成四类，前端按提示前缀分别展示：
//! - 「审核口令错误」（`ADM00007`）：用户输错了，可以重试；
//! - 「审核口令未设置」（`ADM00010`）：该用户还没设置审核口令，去用户管理设置后重试；
//! - 「审核口令校验过于频繁」（`ADM00008`）：短时间错误次数超限，需要等待窗口结束；
//! - 「登录状态已失效」（`ADM00005` / HTTP 401）与「认证服务不可用」（网络、超时、
//!   5xx、未配置 `[api.adm]`）：这两类都不允许删除。
//!
//! 没有单独的关闭开关：adm-rs 是删除动作的前置依赖，未配置时删除会被拒绝，
//! 不会退回「配置口令」或「不校验直接删」的方案。

use adm_api_client::dto::AdmVerifyAuditPasswordDto;
use adm_api_client::get_adm_api_client;
use anyhow::anyhow;
use robotech::api::Ro;
use robotech::api_client::ApiClientError;
use robotech::svc::SvcError;

/// 「审核口令错误」的提示前缀；前端按前缀区分可重试与不可用
pub const WRONG_PASSWORD_PREFIX: &str = "审核口令错误";
/// 「审核口令未设置」的提示前缀；前端按前缀引导去设置
pub const AUDIT_PASSWORD_NOT_SET_PREFIX: &str = "审核口令未设置";
/// 「审核口令校验过于频繁」的提示前缀；前端按前缀提示稍后重试
pub const RATE_LIMITED_PREFIX: &str = "审核口令校验过于频繁";
/// 「登录状态已失效」的提示前缀；前端按前缀清会话并回登录页
pub const SESSION_INVALID_PREFIX: &str = "登录状态已失效";
/// 「认证服务不可用」的提示前缀；前端按前缀区分不可重试的故障
pub const VERIFY_UNAVAILABLE_PREFIX: &str = "认证服务不可用";

/// adm-rs 口令二次确认失败码
const CODE_WRONG_PASSWORD: &str = "ADM00007";
const CODE_RATE_LIMITED: &str = "ADM00008";
const CODE_USER_UNAVAILABLE: &str = "ADM00009";
const CODE_AUDIT_PASSWORD_NOT_SET: &str = "ADM00010";
const CODE_SESSION_INVALID: &str = "ADM00005";

/// # 删除审核口令校验服务
pub struct DbzDeleteVerifySvc;

impl DbzDeleteVerifySvc {
    /// # 校验当前登录用户的审核口令
    ///
    /// * `authorization` - 原样转发给 adm-rs 的认证头（网关透传）；缺失即视为未登录
    /// * `audit_pswd` - 前端算好的 64 位 SM3 摘要
    ///
    /// ## 返回值
    /// * `Ok(())` - 校验通过
    /// * `Err(SvcError)` - 四类失败（提示前缀可区分），调用方不得继续删除
    pub async fn verify(authorization: Option<&str>, audit_pswd: &str) -> Result<(), SvcError> {
        let authorization = authorization
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let Some(authorization) = authorization else {
            return Err(
                anyhow!("{SESSION_INVALID_PREFIX}：未获取到登录令牌，请重新登录后重试").into(),
            );
        };

        let audit_pswd = audit_pswd.trim();
        if audit_pswd.is_empty() {
            return Err(anyhow!("{WRONG_PASSWORD_PREFIX}：审核口令不能为空").into());
        }
        if audit_pswd.len() != 64 || !audit_pswd.chars().all(|ch| ch.is_ascii_hexdigit()) {
            return Err(anyhow!(
                "{WRONG_PASSWORD_PREFIX}：审核口令摘要格式不正确，应为 64 位十六进制"
            )
            .into());
        }

        let client = get_adm_api_client().map_err(|error| {
            anyhow!(
                "{VERIFY_UNAVAILABLE_PREFIX}：未配置 [api.adm] 或客户端未初始化（{error}），删除已被拒绝"
            )
        })?;

        let dto = AdmVerifyAuditPasswordDto {
            audit_pswd: audit_pswd.to_string(),
        };
        let response = match client.auth.verify_audit_password(authorization, &dto).await {
            Ok(response) => response,
            // 非 2xx 也走框架信封：401 是会话失效，其它按认证服务不可用
            Err(ApiClientError::NonSuccessStatus(_, status, body)) => {
                if status.starts_with("401") {
                    return Err(
                        anyhow!("{SESSION_INVALID_PREFIX}：登录令牌已失效，请重新登录").into(),
                    );
                }
                if let Ok(response) = serde_json::from_str::<Ro<()>>(&body) {
                    return Self::check_response(response, &status);
                }
                return Err(anyhow!(
                    "{VERIFY_UNAVAILABLE_PREFIX}：认证服务返回异常响应 HTTP {status}，删除已被拒绝"
                )
                .into());
            }
            // 网络错误、超时、连接被拒：一律按「认证服务不可用」，不能误报成口令错
            Err(error) => {
                return Err(anyhow!(
                    "{VERIFY_UNAVAILABLE_PREFIX}：无法访问认证服务（{error}），删除已被拒绝"
                )
                .into());
            }
        };

        Self::check_response(response, "200")
    }

    /// 按 adm-rs 的业务码映射成前端可区分的提示
    fn check_response(response: Ro<()>, status: &str) -> Result<(), SvcError> {
        if response.is_ok() {
            return Ok(());
        }

        match response.code.as_deref() {
            Some(CODE_WRONG_PASSWORD) => {
                Err(anyhow!("{WRONG_PASSWORD_PREFIX}：当前登录用户的审核口令不正确").into())
            }
            Some(CODE_AUDIT_PASSWORD_NOT_SET) => Err(anyhow!(
                "{AUDIT_PASSWORD_NOT_SET_PREFIX}：当前用户尚未设置审核口令，请先在用户管理中设置"
            )
            .into()),
            Some(CODE_RATE_LIMITED) => {
                Err(anyhow!("{RATE_LIMITED_PREFIX}：短时间内错误尝试过多，请稍后重试").into())
            }
            Some(CODE_USER_UNAVAILABLE) => Err(anyhow!(
                "{SESSION_INVALID_PREFIX}：当前用户不可用，请重新登录或联系管理员"
            )
            .into()),
            Some(CODE_SESSION_INVALID) => {
                Err(anyhow!("{SESSION_INVALID_PREFIX}：登录令牌已失效，请重新登录").into())
            }
            _ => {
                let detail = response.msg.trim();
                let detail = if detail.is_empty() {
                    String::new()
                } else {
                    format!("（{detail}）")
                };
                Err(anyhow!(
                    "{VERIFY_UNAVAILABLE_PREFIX}：认证服务返回异常响应 HTTP {status}{detail}，删除已被拒绝"
                )
                .into())
            }
        }
    }
}
