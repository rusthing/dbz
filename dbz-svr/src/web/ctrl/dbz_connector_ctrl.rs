//! # 连接器控制器
//!
//! 暴露 Kafka Connect 侧的运维接口：查看连接器插件、查看所有连接器、
//! 重启连接器（可连同其任务）与重启单个任务。
//!
//! 本模块没有对应的数据库实体，因此不使用 `#[ctrl]` 宏，直接手写处理器函数；
//! 路由在 `web/router/dbz_connector_router.rs` 中通过 `routes [...]` 注册。

use crate::dto::{DbzConnectorQueryDto, DbzConnectorRestartDto, DbzConnectorTaskRestartDto};
use crate::svc::DbzConnectorSvc;
use crate::vo::{ConnectorPluginVo, ConnectorVo};
use axum::debug_handler;
use axum::extract::Query;
use axum::response::Json;
use robotech::api::Ro;
use robotech::macros::log_call;
use robotech::web::CtrlError;

/// # 查看连接器插件
///
/// 查询目标 Kafka Connect 已安装的连接器插件，用于确认 Debezium 插件是否就绪。
///
/// ## 查询参数
/// * `instanceId` - 实例ID（与 `kafkaConnectUrl` 二选一）
/// * `kafkaConnectUrl` - Kafka Connect 地址（优先于 `instanceId`）
///
/// ## 返回值
/// * 成功时返回插件列表（类名、类型、版本）
/// * 失败时返回相应的错误信息
#[utoipa::path(
    get,
    path = "/dbz/connector/plugins",
    params(DbzConnectorQueryDto),
    responses((status = OK, body = Ro<Vec<ConnectorPluginVo>>))
)]
#[debug_handler]
#[log_call]
pub async fn list_connector_plugins(
    Query(dto): Query<DbzConnectorQueryDto>,
) -> Result<Json<Ro<Vec<ConnectorPluginVo>>>, CtrlError> {
    let ro = DbzConnectorSvc::list_connector_plugins(dto).await?;
    Ok(Json(ro))
}

/// # 查看所有连接器
///
/// 查询目标 Kafka Connect 上的全部连接器，含连接器与各任务的运行状态。
///
/// ## 查询参数
/// * `instanceId` - 实例ID（与 `kafkaConnectUrl` 二选一）
/// * `kafkaConnectUrl` - Kafka Connect 地址（优先于 `instanceId`）
///
/// ## 返回值
/// * 成功时返回连接器列表（名称、类名、类型、状态、任务列表）
/// * 失败时返回相应的错误信息
#[utoipa::path(
    get,
    path = "/dbz/connector/list",
    params(DbzConnectorQueryDto),
    responses((status = OK, body = Ro<Vec<ConnectorVo>>))
)]
#[debug_handler]
#[log_call]
pub async fn list_connectors(
    Query(dto): Query<DbzConnectorQueryDto>,
) -> Result<Json<Ro<Vec<ConnectorVo>>>, CtrlError> {
    let ro = DbzConnectorSvc::list_connectors(dto).await?;
    Ok(Json(ro))
}

/// # 重启连接器
///
/// 重启指定连接器，默认连同其任务一起重启。Kafka Connect 的重启是异步的，
/// 接口返回成功仅代表重启请求已被接受，实际状态请稍后重新查询。
///
/// ## 请求体
/// * `DbzConnectorRestartDto` - 目标 Kafka Connect 定位参数 + 连接器名称 + 重启选项
///
/// ## 返回值
/// * 成功时返回提交结果
/// * 失败时返回相应的错误信息
#[utoipa::path(
    post,
    path = "/dbz/connector/restart",
    request_body = DbzConnectorRestartDto,
    // utoipa 无法解析 `Ro<()>`（空元组没有类型路径），故只描述状态码
    responses((status = OK, description = "重启请求已提交"))
)]
#[debug_handler]
#[log_call]
pub async fn restart_connector(
    Json(dto): Json<DbzConnectorRestartDto>,
) -> Result<Json<Ro<()>>, CtrlError> {
    let ro = DbzConnectorSvc::restart_connector(dto).await?;
    Ok(Json(ro))
}

/// # 重启连接器任务
///
/// 仅重启指定任务，适用于连接器本身正常、个别任务失败（FAILED）的场景。
///
/// ## 请求体
/// * `DbzConnectorTaskRestartDto` - 目标 Kafka Connect 定位参数 + 连接器名称 + 任务ID
///
/// ## 返回值
/// * 成功时返回提交结果
/// * 失败时返回相应的错误信息
#[utoipa::path(
    post,
    path = "/dbz/connector/task/restart",
    request_body = DbzConnectorTaskRestartDto,
    // utoipa 无法解析 `Ro<()>`（空元组没有类型路径），故只描述状态码
    responses((status = OK, description = "重启请求已提交"))
)]
#[debug_handler]
#[log_call]
pub async fn restart_connector_task(
    Json(dto): Json<DbzConnectorTaskRestartDto>,
) -> Result<Json<Ro<()>>, CtrlError> {
    let ro = DbzConnectorSvc::restart_connector_task(dto).await?;
    Ok(Json(ro))
}
