//! # 上游连接器再同步控制器
//!
//! 两个运维接口：预检（哪些表会被快照）与触发（往源库信号表写信号）。
//!
//! 本模块不对应数据库实体，因此不使用 `#[ctrl]` 宏，直接手写处理器函数 ——
//! 与 [`dbz_kafka_connector_ctrl`](crate::web::ctrl::dbz_kafka_connector_ctrl) 一致。
//!
//! 路径挂在 `/dbz/source-connector/snapshot` 下：快照是针对某条上游连接器记录
//! 的操作，跟着它走最直观。`/snapshot/preview` 与 CRUD 的
//! `/dbz/source-connector/{id}` 在静态段上不冲突（静态段优先匹配）。

use crate::dto::{DbzSourceConnectorSnapshotDto, DbzSourceConnectorSnapshotPreviewDto};
use crate::svc::DbzSourceConnectorSnapshotSvc;
use crate::vo::DbzSourceConnectorSnapshotVo;
use axum::debug_handler;
use axum::extract::Query;
use axum::response::Json;
use robotech::api::Ro;
use robotech::macros::log_call;
use robotech::web::CtrlError;

/// # 预检快照范围
///
/// 解析连接器配置并读一次源库，返回方言、信号表与「这次会快照哪些表」。
/// 只读，不会写信号，可以放心反复调用。
///
/// ## 查询参数
/// * `id` - 上游连接器ID
///
/// ## 返回值
/// * 成功时返回方言、库、信号表、表清单与提醒
/// * 失败时返回原因（记录不存在/已禁用、缺 signal.data.collection、连不上源库）
#[utoipa::path(
    get,
    path = "/dbz/source-connector/snapshot/preview",
    params(DbzSourceConnectorSnapshotPreviewDto),
    responses((status = OK, body = Ro<DbzSourceConnectorSnapshotVo>))
)]
#[debug_handler]
#[log_call]
pub async fn preview_snapshot(
    Query(dto): Query<DbzSourceConnectorSnapshotPreviewDto>,
) -> Result<Json<Ro<DbzSourceConnectorSnapshotVo>>, CtrlError> {
    let ro = DbzSourceConnectorSnapshotSvc::preview(dto).await?;
    Ok(Json(ro))
}

/// # 触发一次快照（重新同步）
///
/// 往源库的 `signal.data.collection` 表写一条 `execute-snapshot` 信号，让 Debezium
/// 对指定表再跑一次快照。返回成功只代表信号已写入，快照由连接器异步执行。
///
/// ## 请求体
/// * `DbzSourceConnectorSnapshotDto` - 上游连接器ID、表清单（空=捕获范围全部）、快照类型
///
/// ## 返回值
/// * 成功时返回提交结果（含信号 id 与表数量）
/// * 失败时返回原因（表不在捕获范围、连不上源库、信号表不存在/无权限）
#[utoipa::path(
    post,
    path = "/dbz/source-connector/snapshot",
    request_body = DbzSourceConnectorSnapshotDto,
    // utoipa 无法解析 `Ro<()>`（空元组没有类型路径），故只描述状态码
    responses((status = OK, description = "快照信号已写入源库信号表"))
)]
#[debug_handler]
#[log_call]
pub async fn execute_snapshot(
    Json(dto): Json<DbzSourceConnectorSnapshotDto>,
) -> Result<Json<Ro<()>>, CtrlError> {
    let ro = DbzSourceConnectorSnapshotSvc::snapshot(dto).await?;
    Ok(Json(ro))
}
