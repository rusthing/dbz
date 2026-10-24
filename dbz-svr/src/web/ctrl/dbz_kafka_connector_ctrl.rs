//! # Kafka 连接器控制器
//!
//! 暴露 Kafka Connect 侧的运维接口：查看连接器插件、查看所有连接器、
//! 重启连接器（可连同其任务）、重启单个任务与删除连接器。
//!
//! 本模块没有对应的数据库实体，因此不使用 `#[ctrl]` 宏，直接手写处理器函数。
//!
//! ## 文件名为什么带 `dbz_` 前缀
//!
//! `#[router]` 与 `#[api_doc]` 都按「结构体名去掉后缀再加 `_ctrl`」推导 ctrl 模块，
//! 而 `#[api_doc]` 还会把结构体名转成文档地址（`/dbz-kafka-connector/openapi.json`）。
//! 想让文档地址保住 `/dbz` 这个服务前缀，结构体只能是 `DbzKafkaConnector*`，
//! ctrl 文件名也就必须跟着叫 `dbz_kafka_connector_ctrl.rs`，与 router、api_doc 保持一致。

use crate::dto::{
    KafkaConnectorDeleteDto, KafkaConnectorQueryDto, KafkaConnectorRestartDto,
    KafkaConnectorTaskRestartDto,
};
use crate::svc::KafkaConnectorSvc;
use crate::vo::{KafkaConnectorPluginVo, KafkaConnectorVo};
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
/// * `kafkaConnectId` - Kafka Connect ID（与 `kafkaConnectUrl` 二选一）
/// * `kafkaConnectUrl` - Kafka Connect 地址（优先于 `kafkaConnectId`）
///
/// ## 返回值
/// * 成功时返回插件列表（类名、类型、版本）
/// * 失败时返回相应的错误信息
#[utoipa::path(
    get,
    path = "/dbz/kafka-connector/plugins",
    params(KafkaConnectorQueryDto),
    responses((status = OK, body = Ro<Vec<KafkaConnectorPluginVo>>))
)]
#[debug_handler]
#[log_call]
pub async fn list_connector_plugins(
    Query(dto): Query<KafkaConnectorQueryDto>,
) -> Result<Json<Ro<Vec<KafkaConnectorPluginVo>>>, CtrlError> {
    let ro = KafkaConnectorSvc::list_connector_plugins(dto).await?;
    Ok(Json(ro))
}

/// # 查看所有连接器
///
/// 查询目标 Kafka Connect 上的全部连接器，含连接器与各任务的运行状态。
///
/// ## 查询参数
/// * `kafkaConnectId` - Kafka Connect ID（与 `kafkaConnectUrl` 二选一）
/// * `kafkaConnectUrl` - Kafka Connect 地址（优先于 `kafkaConnectId`）
///
/// ## 返回值
/// * 成功时返回连接器列表（名称、类名、类型、状态、任务列表）
/// * 失败时返回相应的错误信息
#[utoipa::path(
    get,
    path = "/dbz/kafka-connector/list",
    params(KafkaConnectorQueryDto),
    responses((status = OK, body = Ro<Vec<KafkaConnectorVo>>))
)]
#[debug_handler]
#[log_call]
pub async fn list_connectors(
    Query(dto): Query<KafkaConnectorQueryDto>,
) -> Result<Json<Ro<Vec<KafkaConnectorVo>>>, CtrlError> {
    let ro = KafkaConnectorSvc::list_connectors(dto).await?;
    Ok(Json(ro))
}

/// # 重启连接器
///
/// 重启指定连接器，默认连同其任务一起重启。Kafka Connect 的重启是异步的，
/// 接口返回成功仅代表重启请求已被接受，实际状态请稍后重新查询。
///
/// ## 请求体
/// * `KafkaConnectorRestartDto` - 目标 Kafka Connect 定位参数 + 连接器名称 + 重启选项
///
/// ## 返回值
/// * 成功时返回提交结果
/// * 失败时返回相应的错误信息
#[utoipa::path(
    post,
    path = "/dbz/kafka-connector/restart",
    request_body = KafkaConnectorRestartDto,
    // utoipa 无法解析 `Ro<()>`（空元组没有类型路径），故只描述状态码
    responses((status = OK, description = "重启请求已提交"))
)]
#[debug_handler]
#[log_call]
pub async fn restart_connector(
    Json(dto): Json<KafkaConnectorRestartDto>,
) -> Result<Json<Ro<()>>, CtrlError> {
    let ro = KafkaConnectorSvc::restart_connector(dto).await?;
    Ok(Json(ro))
}

/// # 重启连接器任务
///
/// 仅重启指定任务，适用于连接器本身正常、个别任务失败（FAILED）的场景。
///
/// ## 请求体
/// * `KafkaConnectorTaskRestartDto` - 目标 Kafka Connect 定位参数 + 连接器名称 + 任务ID
///
/// ## 返回值
/// * 成功时返回提交结果
/// * 失败时返回相应的错误信息
#[utoipa::path(
    post,
    path = "/dbz/kafka-connector/task/restart",
    request_body = KafkaConnectorTaskRestartDto,
    // utoipa 无法解析 `Ro<()>`（空元组没有类型路径），故只描述状态码
    responses((status = OK, description = "重启请求已提交"))
)]
#[debug_handler]
#[log_call]
pub async fn restart_connector_task(
    Json(dto): Json<KafkaConnectorTaskRestartDto>,
) -> Result<Json<Ro<()>>, CtrlError> {
    let ro = KafkaConnectorSvc::restart_connector_task(dto).await?;
    Ok(Json(ro))
}

/// # 删除连接器
///
/// 把指定连接器从目标集群上注销。接口只操作集群、不落库：删掉的连接器若仍被 dbz
/// 纳管，下一轮健康检查会按记录重新部署回来；要彻底停掉请禁用对应记录。
///
/// ## 查询参数
/// * `kafkaConnectId` - Kafka Connect ID（与 `kafkaConnectUrl` 二选一）
/// * `kafkaConnectUrl` - Kafka Connect 地址（优先于 `kafkaConnectId`）
/// * `connectorName` - 连接器名称
///
/// ## 返回值
/// * 成功时返回删除结果（连接器本就不存在也算成功）
/// * 失败时返回相应的错误信息
#[utoipa::path(
    delete,
    path = "/dbz/kafka-connector",
    params(KafkaConnectorDeleteDto),
    // utoipa 无法解析 `Ro<()>`（空元组没有类型路径），故只描述状态码
    responses((status = OK, description = "连接器已从集群上删除"))
)]
#[debug_handler]
#[log_call]
pub async fn delete_connector(
    Query(dto): Query<KafkaConnectorDeleteDto>,
) -> Result<Json<Ro<()>>, CtrlError> {
    let ro = KafkaConnectorSvc::delete_connector(dto).await?;
    Ok(Json(ro))
}
