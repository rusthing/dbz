//! # 死信控制器
//!
//! 六个手写接口：
//! - `POST /dbz/dead-letter/report`：magic 脚本上报失败（内部写入）
//! - `GET  /dbz/dead-letter/page`：分页查询
//! - `GET  /dbz/dead-letter/pending-count`：待处理计数（前端角标）
//! - `POST /dbz/dead-letter/retry`：单条/批量重投（可带修改后的报文）
//! - `POST /dbz/dead-letter/edit-payload`：保存待投报文（不重投）
//! - `POST /dbz/dead-letter/discard`：单条/批量废弃
//! - `POST /dbz/dead-letter/mark-handled`：批量标记「已人工处理」
//! - `POST /dbz/dead-letter/unmark-handled`：批量撤销「已人工处理」
//!
//! 上报接口允许不带 `X-User-Id`（脚本没有登录态），其余接口沿用框架约定，
//! 缺少用户头按参数校验失败处理。

use crate::dto::{
    DbzDeadLetterCountDto, DbzDeadLetterDiscardDto, DbzDeadLetterEditPayloadDto,
    DbzDeadLetterMarkHandledDto, DbzDeadLetterQueryDto, DbzDeadLetterReportDto,
    DbzDeadLetterRetryDto, DbzDeadLetterUnmarkHandledDto,
};
use crate::svc::DbzDeadLetterSvc;
use crate::vo::{DbzDeadLetterCountVo, DbzDeadLetterRetryResultVo, DbzDeadLetterVo};
use axum::debug_handler;
use axum::extract::Query;
use axum::http::HeaderMap;
use axum::response::Json;
use robotech::api::{PageRx, Ro, U64};
use robotech::macros::log_call;
use robotech::web::ctrl_utils::get_current_user_id;
use robotech::web::CtrlError;

/// 脚本上报允许没有用户头，缺省记为 0（系统）
fn optional_operator(headers: &HeaderMap) -> U64 {
    get_current_user_id(headers).unwrap_or(U64(0))
}

/// 前端操作必须带用户头
fn required_operator(headers: &HeaderMap) -> Result<U64, CtrlError> {
    Ok(get_current_user_id(headers)?)
}

/// # 上报死信
///
/// magic 脚本的失败分支调用。确定性失败直接进死信；传输类失败进重试队列，
/// 由后台 worker 按 1s→5s→30s→2m→10m 退避重投，超过 10 次转死信。
/// 响应异常（`result` 非成功且无法归类）按传输类处理。
#[utoipa::path(
    post,
    path = "/dbz/dead-letter/report",
    request_body = DbzDeadLetterReportDto,
    responses((status = OK, description = "已写入死信或重试队列"))
)]
#[debug_handler]
#[log_call]
pub async fn report(
    headers: HeaderMap,
    Json(dto): Json<DbzDeadLetterReportDto>,
) -> Result<Json<Ro<()>>, CtrlError> {
    let ro = DbzDeadLetterSvc::report(dto, optional_operator(&headers)).await?;
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
/// 把报文重新 POST 到下游连接器配置里的 magic sync 地址；HTTP 2xx 且
/// `result=1` 才转「已重投」，失败保持待处理并把原因回写到记录。单条重投只传一个 ID。
/// 可选 `payload` 为修改后的报文（JSON 文本）；仅单条、且记录必须是「确定性失败 +
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
/// 传输类、响应异常类以及非字段不合规的确定性失败都会被接口拒绝。
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
