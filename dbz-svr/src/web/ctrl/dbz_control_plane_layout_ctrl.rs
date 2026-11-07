//! # 控制面画布布局控制器

use crate::dto::{DbzControlPlaneLayoutDeleteDto, DbzControlPlaneLayoutUpsertDto};
use crate::svc::DbzControlPlaneLayoutSvc;
use crate::vo::DbzControlPlaneLayoutVo;
use axum::debug_handler;
use axum::extract::Query;
use axum::http::HeaderMap;
use axum::response::Json;
use robotech::api::Ro;
use robotech::macros::log_call;
use robotech::web::CtrlError;
use robotech::web::ctrl_utils::get_current_user_id;

/// # 读取全部节点位置
#[utoipa::path(
    get,
    path = "/dbz/control-plane/layout",
    responses((status = OK, body = Ro<Vec<DbzControlPlaneLayoutVo>>))
)]
#[debug_handler]
#[log_call]
pub async fn list() -> Result<Json<Ro<Vec<DbzControlPlaneLayoutVo>>>, CtrlError> {
    let ro = DbzControlPlaneLayoutSvc::list().await?;
    Ok(Json(ro))
}

/// # 保存一个节点的位置
#[utoipa::path(
    put,
    path = "/dbz/control-plane/layout",
    request_body = DbzControlPlaneLayoutUpsertDto,
    responses((status = OK, description = "已保存"))
)]
#[debug_handler]
#[log_call]
pub async fn upsert(
    headers: HeaderMap,
    Json(dto): Json<DbzControlPlaneLayoutUpsertDto>,
) -> Result<Json<Ro<()>>, CtrlError> {
    let ro = DbzControlPlaneLayoutSvc::upsert(dto, get_current_user_id(&headers)?).await?;
    Ok(Json(ro))
}

/// # 删除一个节点的位置
#[utoipa::path(
    delete,
    path = "/dbz/control-plane/layout",
    params(DbzControlPlaneLayoutDeleteDto),
    responses((status = OK, description = "已删除"))
)]
#[debug_handler]
#[log_call]
pub async fn remove(
    headers: HeaderMap,
    Query(dto): Query<DbzControlPlaneLayoutDeleteDto>,
) -> Result<Json<Ro<()>>, CtrlError> {
    // 删除同样校验用户头，保持写接口的权限约定一致
    let _ = get_current_user_id(&headers)?;
    let ro = DbzControlPlaneLayoutSvc::delete(dto).await?;
    Ok(Json(ro))
}
