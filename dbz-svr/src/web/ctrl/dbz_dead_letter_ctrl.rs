//! # 死信控制器
//!
//! 八个手写接口（死信由 Kafka Connect DLQ 产生，没有「上报」入口）：
//! - `POST /dbz/dead-letter/sync`：立即从 DLQ topic 同步一次
//! - `GET  /dbz/dead-letter/page`：分页查询
//! - `GET  /dbz/dead-letter/pending-count`：待处理计数（前端角标）
//! - `POST /dbz/dead-letter/retry`：单条/批量重投（写回源 topic，可带修改后报文）
//! - `POST /dbz/dead-letter/edit-payload`：保存待投报文（不重投）
//! - `POST /dbz/dead-letter/discard`：单条/批量废弃
//! - `POST /dbz/dead-letter/mark-handled`：批量标记「已人工处理」
//! - `POST /dbz/dead-letter/unmark-handled`：批量撤销「已人工处理」
//!
//! 所有写操作沿用框架约定，缺少 `X-User-Id` 按参数校验失败处理。

use crate::dto::{
    DbzDeadLetterCountDto, DbzDeadLetterDiscardDto, DbzDeadLetterEditPayloadDto,
    DbzDeadLetterMarkHandledDto, DbzDeadLetterQueryDto, DbzDeadLetterRetryDto,
    DbzDeadLetterUnmarkHandledDto,
};
use crate::svc::DbzDeadLetterSvc;
use crate::vo::{DbzDeadLetterCountVo, DbzDeadLetterRetryResultVo, DbzDeadLetterVo};
use axum::debug_handler;
use axum::extract::Query;
use axum::http::HeaderMap;
use axum::response::Json;
use robotech::api::{PageRx, Ro, U64};
use robotech::macros::log_call;
use robotech::web::CtrlError;
use robotech::web::ctrl_utils::get_current_user_id;

/// 前端操作必须带用户头
// CtrlError 由框架定义且体积较大，这里的返回类型要和其它控制器保持一致
#[allow(clippy::result_large_err)]
fn required_operator(headers: &HeaderMap) -> Result<U64, CtrlError> {
    Ok(get_current_user_id(headers)?)
}

/// # 立即同步 Kafka Connect DLQ
///
/// 正常由 jobx 周期任务自动同步，这里给运维一个「马上看最新坏记录」的入口。
/// 返回本轮新落库的条数；DLQ topic 里的原始记录不受影响。
#[utoipa::path(
    post,
    path = "/dbz/dead-letter/sync",
    responses((status = OK, body = Ro<u64>))
)]
#[debug_handler]
#[log_call]
pub async fn sync(headers: HeaderMap) -> Result<Json<Ro<u64>>, CtrlError> {
    let _ = required_operator(&headers)?;
    let ro = DbzDeadLetterSvc::sync().await?;
    Ok(Json(ro))
}

/// # 分页查询死信
///
/// 支持按连接器类型、连接器 ID、状态、topic 与末次失败时间筛选。
#[utoipa::path(
    get,
    path = "/dbz/dead-letter/page",
    params(DbzDeadLetterQueryDto),
    responses((status = OK, body = Ro<PageRx<DbzDeadLetterVo>>))
)]
#[debug_handler]
#[log_call]
pub async fn page(
    Query(dto): Query<DbzDeadLetterQueryDto>,
) -> Result<Json<Ro<PageRx<DbzDeadLetterVo>>>, CtrlError> {
    let ro = DbzDeadLetterSvc::page(dto).await?;
    Ok(Json(ro))
}

/// # 待处理死信计数
///
/// 不传 `connectorId` 时按连接器分组返回全部，供控制面一次请求挂上所有角标。
#[utoipa::path(
    get,
    path = "/dbz/dead-letter/pending-count",
    params(DbzDeadLetterCountDto),
    responses((status = OK, body = Ro<Vec<DbzDeadLetterCountVo>>))
)]
#[debug_handler]
#[log_call]
pub async fn pending_count(
    Query(dto): Query<DbzDeadLetterCountDto>,
) -> Result<Json<Ro<Vec<DbzDeadLetterCountVo>>>, CtrlError> {
    let ro = DbzDeadLetterSvc::pending_count(dto).await?;
    Ok(Json(ro))
}

/// # 批量重投
///
/// 把报文重新生产回源 topic，由 Kafka Connect 再消费一次；生产成功即转「已重投」，
/// 失败保持待处理并把原因回写到记录。单条重投只传一个 ID。
/// 可选 `payload` 为修改后的报文（JSON 文本）；仅单条、且记录必须是「数据/格式类 +
/// 字段不合规」时允许，原始报文始终保留。
#[utoipa::path(
    post,
    path = "/dbz/dead-letter/retry",
    request_body = DbzDeadLetterRetryDto,
    responses((status = OK, body = Ro<Vec<DbzDeadLetterRetryResultVo>>))
)]
#[debug_handler]
#[log_call]
pub async fn retry(
    headers: HeaderMap,
    Json(dto): Json<DbzDeadLetterRetryDto>,
) -> Result<Json<Ro<Vec<DbzDeadLetterRetryResultVo>>>, CtrlError> {
    let ro = DbzDeadLetterSvc::retry(dto, required_operator(&headers)?).await?;
    Ok(Json(ro))
}

/// # 保存待投报文
///
/// 只写 `pending_payload` 与编辑人/编辑时间，不重投；原始报文不覆盖。
/// 投递类、其它类以及非字段不合规的数据/格式类异常都会被接口拒绝。
#[utoipa::path(
    post,
    path = "/dbz/dead-letter/edit-payload",
    request_body = DbzDeadLetterEditPayloadDto,
    responses((status = OK, body = Ro<DbzDeadLetterVo>))
)]
#[debug_handler]
#[log_call]
pub async fn edit_payload(
    headers: HeaderMap,
    Json(dto): Json<DbzDeadLetterEditPayloadDto>,
) -> Result<Json<Ro<DbzDeadLetterVo>>, CtrlError> {
    let ro = DbzDeadLetterSvc::edit_payload(dto, required_operator(&headers)?).await?;
    Ok(Json(ro))
}

/// # 批量废弃
///
/// 标记为「已废弃」并记录处理人与备注，不删行。
#[utoipa::path(
    post,
    path = "/dbz/dead-letter/discard",
    request_body = DbzDeadLetterDiscardDto,
    responses((status = OK, description = "已废弃"))
)]
#[debug_handler]
#[log_call]
pub async fn discard(
    headers: HeaderMap,
    Json(dto): Json<DbzDeadLetterDiscardDto>,
) -> Result<Json<Ro<()>>, CtrlError> {
    let ro = DbzDeadLetterSvc::discard(dto, required_operator(&headers)?).await?;
    Ok(Json(ro))
}

/// # 批量标记「已人工处理」
///
/// 记录处理说明、处理人与时间；只对「待处理」的记录生效。与「已废弃」区分：
/// 人工处理表示问题已在别处解决，废弃表示不再处理。
#[utoipa::path(
    post,
    path = "/dbz/dead-letter/mark-handled",
    request_body = DbzDeadLetterMarkHandledDto,
    responses((status = OK, description = "已标记人工处理"))
)]
#[debug_handler]
#[log_call]
pub async fn mark_handled(
    headers: HeaderMap,
    Json(dto): Json<DbzDeadLetterMarkHandledDto>,
) -> Result<Json<Ro<()>>, CtrlError> {
    let ro = DbzDeadLetterSvc::mark_handled(dto, required_operator(&headers)?).await?;
    Ok(Json(ro))
}

/// # 批量撤销「已人工处理」
///
/// 把状态恢复为「待处理」并清空处理人与处理说明；已废弃、已重投的记录不受影响。
#[utoipa::path(
    post,
    path = "/dbz/dead-letter/unmark-handled",
    request_body = DbzDeadLetterUnmarkHandledDto,
    responses((status = OK, description = "已撤销人工处理"))
)]
#[debug_handler]
#[log_call]
pub async fn unmark_handled(
    headers: HeaderMap,
    Json(dto): Json<DbzDeadLetterUnmarkHandledDto>,
) -> Result<Json<Ro<()>>, CtrlError> {
    let ro = DbzDeadLetterSvc::unmark_handled(dto, required_operator(&headers)?).await?;
    Ok(Json(ro))
}
