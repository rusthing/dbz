//! # 删除口令校验
//!
//! 删除画布节点前做二次确认，校验的是**当前登录用户**的口令，不是某个独立配置项。
//! dbz 不自己保管口令，而是把登录令牌与口令摘要原样转发给 adm-rs：
//!
//! ```text
//! POST {adm-base-url}{path}
//! Authorization: <网关透传的 Bearer 令牌>
//! {"loginPswd": "<SM3 明文后的 64 位十六进制>"}
//! ```
//!
//! 身份只取自 `Authorization`，请求体里不接受账号，也不带 `X-User-Id`——adm-rs
//! 明确以令牌里的用户为准。adm-rs 只做口令核对，**不累加登录失败次数、不锁号**。
//!
//! 失败必须能区分成三类，前端按提示前缀分别展示：
//! - 「口令错误」（`ADM00007`）：用户输错了，可以重试；
//! - 「口令校验过于频繁」（`ADM00008`）：短时间错误次数超限，需要等待窗口结束；
//! - 「登录状态已失效」（`ADM00005` / HTTP 401）与「认证服务不可用」（网络、超时、
//!   5xx、未配置）：这两类都不允许删除。
//!
//! adm-rs 接口就绪前用 `[dbz.delete-verify].enabled = false` 过渡；关闭后删除
//! 直接放行，不会退回「配置口令」方案。

use crate::config::get_dbz_config;
use anyhow::anyhow;
use robotech::svc::SvcError;
use std::time::Duration;

/// 「口令错误」的提示前缀；前端按前缀区分可重试与不可用
pub const WRONG_PASSWORD_PREFIX: &str = "口令错误";
/// 「口令校验过于频繁」的提示前缀；前端按前缀提示稍后重试
pub const RATE_LIMITED_PREFIX: &str = "口令校验过于频繁";
/// 「登录状态已失效」的提示前缀；前端按前缀清会话并回登录页
pub const SESSION_INVALID_PREFIX: &str = "登录状态已失效";
/// 「认证服务不可用」的提示前缀；前端按前缀区分不可重试的故障
pub const VERIFY_UNAVAILABLE_PREFIX: &str = "认证服务不可用";

/// adm-rs 口令二次确认失败码
const CODE_WRONG_PASSWORD: &str = "ADM00007";
const CODE_RATE_LIMITED: &str = "ADM00008";
const CODE_USER_UNAVAILABLE: &str = "ADM00009";
const CODE_SESSION_INVALID: &str = "ADM00005";

/// # 删除口令校验服务
pub struct DbzDeleteVerifySvc;

impl DbzDeleteVerifySvc {
    /// # 校验当前登录用户的口令
    ///
    /// * `authorization` - 原样转发给 adm-rs 的认证头（网关透传）；缺失即视为未登录
    /// * `login_pswd` - 前端算好的 64 位 SM3 摘要
    ///
    /// ## 返回值
    /// * `Ok(())` - 校验通过，或过渡开关已关闭
    /// * `Err(SvcError)` - 三类失败（提示前缀可区分），调用方不得继续删除
    pub async fn verify(authorization: Option<&str>, login_pswd: &str) -> Result<(), SvcError> {
        let config = get_dbz_config().map_err(|e| anyhow!("读取 dbz 配置失败: {e}"))?;
        if !config.delete_verify.enabled {
            return Ok(());
        }

        let authorization = authorization.map(str::trim).filter(|value| !value.is_empty());
        let Some(authorization) = authorization else {
            return Err(anyhow!(
                "{SESSION_INVALID_PREFIX}：未获取到登录令牌，请重新登录后重试"
            )
            .into());
        };

        let login_pswd = login_pswd.trim();
        if login_pswd.is_empty() {
            return Err(anyhow!("{WRONG_PASSWORD_PREFIX}：口令不能为空").into());
        }
        if login_pswd.len() != 64 || !login_pswd.chars().all(|ch| ch.is_ascii_hexdigit()) {
            return Err(anyhow!(
                "{WRONG_PASSWORD_PREFIX}：口令摘要格式不正确，应为 64 位十六进制"
            )
            .into());
        }

        let base_url = config.delete_verify.adm_base_url.trim().trim_end_matches('/');
        if base_url.is_empty() {
            return Err(anyhow!(
                "{VERIFY_UNAVAILABLE_PREFIX}：未配置 [dbz.delete-verify].adm-base-url，删除已被拒绝"
            )
            .into());
        }

        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(
                config.delete_verify.timeout_ms.max(500),
            ))
            .build()
            .map_err(|e| anyhow!("{VERIFY_UNAVAILABLE_PREFIX}：初始化校验客户端失败（{e}）"))?;

        let url = format!("{base_url}{}", config.delete_verify.path);
        let response = match client
            .post(&url)
            .header("Authorization", authorization)
            .json(&serde_json::json!({ "loginPswd": login_pswd }))
            .send()
            .await
        {
            Ok(response) => response,
            // 网络错误、超时、连接被拒：一律按「认证服务不可用」，不能误报成口令错
            Err(error) => {
                return Err(anyhow!(
                    "{VERIFY_UNAVAILABLE_PREFIX}：无法访问认证服务（{error}），删除已被拒绝"
                )
                .into());
            }
        };

        let status = response.status();
        let body: serde_json::Value = response.json().await.unwrap_or(serde_json::Value::Null);
        if Self::is_success(&body) {
            return Ok(());
        }

        let code = body.get("code").and_then(serde_json::Value::as_str);
        match code {
            Some(CODE_WRONG_PASSWORD) => {
                Err(anyhow!("{WRONG_PASSWORD_PREFIX}：当前登录用户口令不正确").into())
            }
            Some(CODE_RATE_LIMITED) => Err(anyhow!(
                "{RATE_LIMITED_PREFIX}：短时间内错误尝试过多，请稍后重试"
            )
            .into()),
            Some(CODE_USER_UNAVAILABLE) => {
                Err(anyhow!("{SESSION_INVALID_PREFIX}：当前用户不可用，请重新登录或联系管理员").into())
            }
            Some(CODE_SESSION_INVALID) => {
                Err(anyhow!("{SESSION_INVALID_PREFIX}：登录令牌已失效，请重新登录").into())
            }
            _ => {
                if status.as_u16() == 401 {
                    return Err(anyhow!(
                        "{SESSION_INVALID_PREFIX}：登录令牌已失效，请重新登录"
                    )
                    .into());
                }
                let detail = body
                    .get("msg")
                    .and_then(serde_json::Value::as_str)
                    .filter(|value| !value.trim().is_empty())
                    .map(|value| format!("（{value}）"))
                    .unwrap_or_default();
                Err(anyhow!(
                    "{VERIFY_UNAVAILABLE_PREFIX}：认证服务返回异常响应 HTTP {status}{detail}，删除已被拒绝"
                )
                .into())
            }
        }
    }

    /// 兼容常见的成功标记：Ro 的 `result=1`、`success=true`、`data=true`
    fn is_success(body: &serde_json::Value) -> bool {
        body.get("result")
            .and_then(serde_json::Value::as_i64)
            .map(|value| value == 1)
            .or_else(|| {
                body.get("success")
                    .and_then(serde_json::Value::as_bool)
            })
            .or_else(|| body.get("data").and_then(serde_json::Value::as_bool))
            .unwrap_or(false)
    }
}
