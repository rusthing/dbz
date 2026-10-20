//! # Kafka 连接器请求 DTO
//!
//! 定义面向 Kafka Connect 的运维接口入参：查看插件、查看所有连接器、重启连接器、
//! 重启单个任务。
//!
//! ## 命名
//!
//! `kafka_` 前缀表示这些结构描述的是**远端 Kafka Connect 上运行的连接器**：不落库、
//! 没有 `mo` 实体，字段只对应 Kafka 的请求与响应。本库记录的 DTO/VO 一律用 `dbz_`
//! 前缀（如 `dbz_source_connector_dto`）。判断原则很简单 —— 带 `Kafka` 的都不是
//! 我们的数据，是集群上事实的镜像，操作它等于发 HTTP；带 `dbz_` 的要查库。
//!
//! ## 定位目标集群
//!
//! 目标 Kafka Connect 通过 `kafkaConnectUrl` 或 `kafkaConnectId` 二者之一定位：
//! - `kafkaConnectUrl` - 直接指定地址，适合排查未被任何记录纳管的集群；
//! - `kafkaConnectId` - 取 `dbz_kafka_connect` 记录上的地址，适合从 Kafka Connect
//!   或连接器列表发起操作。
//!
//! 同时提供时以 `kafkaConnectUrl` 为准。

use robotech::api::U64;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

/// # 连接器定位查询 DTO
///
/// 用于查看连接器插件、查看所有连接器这类只读接口。
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema, IntoParams)]
#[serde(default, rename_all = "camelCase")]
pub struct KafkaConnectorQueryDto {
    /// Kafka Connect ID（未提供 kafkaConnectUrl 时必填）
    pub kafka_connect_id: Option<U64>,
    /// Kafka Connect 地址（优先于 kafkaConnectId）
    pub kafka_connect_url: Option<String>,
}

/// # 重启连接器 DTO
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct KafkaConnectorRestartDto {
    /// Kafka Connect ID（未提供 kafkaConnectUrl 时必填）
    #[serde(default)]
    pub kafka_connect_id: Option<U64>,
    /// Kafka Connect 地址（优先于 kafkaConnectId）
    #[serde(default)]
    pub kafka_connect_url: Option<String>,
    /// 连接器名称
    pub connector_name: String,
    /// 是否同时重启该连接器的任务（默认 `true`）
    #[serde(default = "default_include_tasks")]
    pub include_tasks: bool,
    /// 是否只重启失败的任务（默认 `false`，即全部重启）
    #[serde(default)]
    pub only_failed: bool,
}

/// # 重启连接器任务 DTO
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct KafkaConnectorTaskRestartDto {
    /// Kafka Connect ID（未提供 kafkaConnectUrl 时必填）
    #[serde(default)]
    pub kafka_connect_id: Option<U64>,
    /// Kafka Connect 地址（优先于 kafkaConnectId）
    #[serde(default)]
    pub kafka_connect_url: Option<String>,
    /// 连接器名称
    pub connector_name: String,
    /// 任务ID（可先从「查看所有连接器」接口的 `tasks` 中获取）
    pub task_id: u32,
}

/// `includeTasks` 的默认值：重启连接器时默认连同其任务一起重启
fn default_include_tasks() -> bool {
    true
}
