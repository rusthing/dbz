//! # 连接器请求 DTO
//!
//! 定义 Kafka Connect 连接器运维接口的入参：查询目标 Kafka Connect、重启连接器、
//! 重启单个任务。
//!
//! 目标 Kafka Connect 通过 `kafkaConnectUrl` 或 `instanceId` 二者之一定位：
//! - `kafkaConnectUrl` - 直接指定地址，适合排查未被任何实例纳管的连接器；
//! - `instanceId` - 取该实例配置的 Kafka Connect 地址，适合从实例列表发起操作。
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
pub struct DbzConnectorQueryDto {
    /// 实例ID（未提供 kafkaConnectUrl 时必填）
    pub instance_id: Option<U64>,
    /// Kafka Connect 地址（优先于 instanceId）
    pub kafka_connect_url: Option<String>,
}

/// # 重启连接器 DTO
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzConnectorRestartDto {
    /// 实例ID（未提供 kafkaConnectUrl 时必填）
    #[serde(default)]
    pub instance_id: Option<U64>,
    /// Kafka Connect 地址（优先于 instanceId）
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
pub struct DbzConnectorTaskRestartDto {
    /// 实例ID（未提供 kafkaConnectUrl 时必填）
    #[serde(default)]
    pub instance_id: Option<U64>,
    /// Kafka Connect 地址（优先于 instanceId）
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
