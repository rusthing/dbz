//! # 控制面状态控制器
//!
//! 两个入口：进入页面/轮询时触发的状态刷新，以及卡片上的「重置位点」。前端不直连
//! Kafka Connect，三步编排全部由后端完成并逐步返回结果。

use crate::dto::DbzResetConnectorOffsetsDto;
use crate::svc::DbzControlPlaneStatusSvc;
use crate::vo::DbzResetConnectorOffsetsVo;
use axum::debug_handler;
use axum::response::Json;
use robotech::api::Ro;
use robotech::macros::log_call;
use robotech::web::CtrlError;

/// # 刷新全部连接器的统一状态
///
/// 只读不写集群：不存在的连接器记「仅纳管未部署」，已停用的记录不发请求。
#[utoipa::path(
    get,
    path = "/dbz/control-plane/status/refresh",
    responses((status = OK, body = Ro<u64>))
)]
#[debug_handler]
#[log_call]
pub async fn refresh() -> Result<Json<Ro<u64>>, CtrlError> {
    let ro = DbzControlPlaneStatusSvc::refresh().await?;
    Ok(Json(ro))
}

/// # 重置连接器位点
///
/// 顺序固定为「停止 → 删除位点 → 可选恢复」，任一步失败立即返回停在哪一步。
#[utoipa::path(
    post,
    path = "/dbz/control-plane/connector/reset-offsets",
    request_body = DbzResetConnectorOffsetsDto,
    responses((status = OK, body = Ro<DbzResetConnectorOffsetsVo>))
)]
#[debug_handler]
#[log_call]
pub async fn reset_offsets(
    Json(dto): Json<DbzResetConnectorOffsetsDto>,
) -> Result<Json<Ro<DbzResetConnectorOffsetsVo>>, CtrlError> {
    let ro = DbzControlPlaneStatusSvc::reset_offsets(dto).await?;
    Ok(Json(ro))
}
