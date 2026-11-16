//! # 框架 CRUD 删除接口的口令闸门
//!
//! 四个 CRUD 控制器把 `del_by_id` / `del_by_query_dto` 从宏生成的版本换成了这里
//! 的手写版本：路径与方法不变，只是在真正删除前统一校验审核口令。口令摘要走
//! `X-Audit-Pswd` 请求头，`Authorization` 原样转发给 adm-rs。

use crate::svc::{DbzControlledDeleteSvc, DbzDeleteVerifyError, AUDIT_PSWD_HEADER};
use axum::http::{header::AUTHORIZATION, HeaderMap};

/// # 校验本次删除请求的审核口令
///
/// 返回 `Err` 时调用方**不得**继续删除，直接回 `Ro::warn(msg).code(Some(code))`
/// 即可，与 `/dbz/control-plane/node-delete` 的分流口径一致。
pub async fn verify(headers: &HeaderMap) -> Result<(), DbzDeleteVerifyError> {
    let authorization = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    let audit_pswd = headers
        .get(AUDIT_PSWD_HEADER)
        .and_then(|value| value.to_str().ok());
    DbzControlledDeleteSvc::verify(authorization, audit_pswd)
        .await
        .map(|_| ())
}
