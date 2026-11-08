//! # 控制面操作控制器
//!
//! - `GET    /dbz/control-plane/edges`：读取全部连线折点
//! - `PUT    /dbz/control-plane/edges`：保存一条连线的折点
//! - `DELETE /dbz/control-plane/edges`：恢复某条连线的默认走线
//! - `POST   /dbz/control-plane/node-delete`：校验当前用户口令后删除节点
//! - `POST   /dbz/control-plane/connector-config/sync`：对比弹窗里的双向配置同步
//!
//! 删除与同步远端都要求口令：必须带 `Authorization`（口令校验身份）与 `X-User-Id`
//! （审计操作人）；口令摘要放在请求体里，服务端不落库、只转发给 adm-rs。

use crate::dto::{
    DbzConnectorConfigSyncDto, DbzControlPlaneEdgeBendDeleteDto,
    DbzControlPlaneEdgeBendUpsertDto, DbzControlPlaneNodeDeleteDto,
};
use crate::svc::{
    DbzConnectorConfigSyncSvc, DbzControlPlaneDeleteSvc, DbzControlPlaneEdgeLayoutSvc,
};
use crate::vo::{
    DbzConnectorConfigSyncVo, DbzControlPlaneEdgeBendVo, DbzControlPlaneNodeDeleteVo,
};
use axum::debug_handler;
use axum::extract::Query;
use axum::http::HeaderMap;
use axum::response::Json;
use robotech::api::Ro;
use robotech::macros::log_call;
use robotech::web::CtrlError;
use robotech::web::ctrl_utils::get_current_user_id;

/// 转发给 adm-rs 的认证头（网关注入的原始值）
fn authorization_of(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
}

/// # 读取全部连线折点
#[utoipa::path(
    get,
    path = "/dbz/control-plane/edges",
    responses((status = OK, body = Ro<Vec<DbzControlPlaneEdgeBendVo>>))
)]
#[debug_handler]
#[log_call]
pub async fn list_edges() -> Result<Json<Ro<Vec<DbzControlPlaneEdgeBendVo>>>, CtrlError> {
    let ro = DbzControlPlaneEdgeLayoutSvc::list().await?;
    Ok(Json(ro))
}

/// # 保存一条连线的折点
#[utoipa::path(
    put,
    path = "/dbz/control-plane/edges",
    request_body = DbzControlPlaneEdgeBendUpsertDto,
    responses((status = OK, description = "已保存"))
)]
#[debug_handler]
#[log_call]
pub async fn upsert_edge(
    headers: HeaderMap,
    Json(dto): Json<DbzControlPlaneEdgeBendUpsertDto>,
) -> Result<Json<Ro<()>>, CtrlError> {
    let ro = DbzControlPlaneEdgeLayoutSvc::upsert(dto, get_current_user_id(&headers)?).await?;
    Ok(Json(ro))
}

/// # 删除一条连线的折点
#[utoipa::path(
    delete,
    path = "/dbz/control-plane/edges",
    params(DbzControlPlaneEdgeBendDeleteDto),
    responses((status = OK, description = "已恢复默认走线"))
)]
#[debug_handler]
#[log_call]
pub async fn remove_edge(
    headers: HeaderMap,
    Query(dto): Query<DbzControlPlaneEdgeBendDeleteDto>,
) -> Result<Json<Ro<()>>, CtrlError> {
    let _ = get_current_user_id(&headers)?;
    let ro = DbzControlPlaneEdgeLayoutSvc::delete(dto).await?;
    Ok(Json(ro))
}

/// # 校验口令后删除节点
///
/// 口令校验失败时 HTTP 仍是 200，`result` 为 warn 且带业务码：
/// `ADM00007` 口令不正确、`ADM00008` 过于频繁、`ADM00009` 账号不可用、
/// `ADM00005` 登录状态已失效、`DBZ00001` 认证服务不可用。成功时 `extra.pswdKind`
/// 说明本次校验的是审核口令（`audit`）还是登录口令（`login`）。
#[utoipa::path(
    post,
    path = "/dbz/control-plane/node-delete",
    request_body = DbzControlPlaneNodeDeleteDto,
    responses((status = OK, body = Ro<DbzControlPlaneNodeDeleteVo>))
)]
#[debug_handler]
#[log_call]
pub async fn delete_node(
    headers: HeaderMap,
    Json(dto): Json<DbzControlPlaneNodeDeleteDto>,
) -> Result<Json<Ro<DbzControlPlaneNodeDeleteVo>>, CtrlError> {
    let ro = DbzControlPlaneDeleteSvc::delete_node(
        dto,
        get_current_user_id(&headers)?,
        authorization_of(&headers),
    )
    .await?;
    Ok(Json(ro))
}

/// # 双向同步连接器配置
///
/// `toRemote`（库内为准下发远端）要求口令，失败时 HTTP 仍是 200，`result` 为 warn
/// 且带业务码，与删除一致：`ADM00007` 口令不正确、`ADM00008` 过于频繁、
/// `ADM00009` 账号不可用、`ADM00005` 登录状态已失效、`DBZ00001` 认证服务不可用。
/// `toLocal`（远端为准写回库内）只写库内记录，不要求口令。
///
/// 成功后 `extra` 带重新计算的 `inconsistent` 与脱敏快照，前端无需再猜标记。
#[utoipa::path(
    post,
    path = "/dbz/control-plane/connector-config/sync",
    request_body = DbzConnectorConfigSyncDto,
    responses((status = OK, body = Ro<DbzConnectorConfigSyncVo>))
)]
#[debug_handler]
#[log_call]
pub async fn sync_connector_config(
    headers: HeaderMap,
    Json(dto): Json<DbzConnectorConfigSyncDto>,
) -> Result<Json<Ro<DbzConnectorConfigSyncVo>>, CtrlError> {
    let ro = DbzConnectorConfigSyncSvc::sync_config(
        dto,
        get_current_user_id(&headers)?,
        authorization_of(&headers),
    )
    .await?;
    Ok(Json(ro))
}
