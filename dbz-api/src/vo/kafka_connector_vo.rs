//! # Kafka 连接器视图对象
//!
//! 承载 Kafka Connect REST API 返回的连接器信息（插件、连接器及其任务），
//! 供 dbz 的运维接口直接返回给调用方。
//!
//! 注意：这些结构不落库、没有对应的 `mo` 实体，因此**不使用** `#[vo]` 宏
//! （该宏要求存在同名的 sea-orm 实体），而是手写 `ToSchema` / `Serialize`。
//! 字段命名统一为 `camelCase`，与 `#[vo]` 生成的 VO 保持一致。
//!
//! 与 `dbz_*` 前缀的 VO 的区别：那些是库记录的视图，本文件的三个结构是**远端
//! 集群上真实存在的连接器**的快照，字段直接对应 Kafka Connect 的响应结构；
//! 类名统一加 `KafkaConnector` 前缀，避免 `ConnectorVo` 这种在扁平 `vo` 命名空间里
//! 看不出归属的泛化名字。

use crate::dic::KafkaConnectorStatus;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// # 连接器插件 VO
///
/// 对应 `GET /connector-plugins` 返回的单个插件信息。
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct KafkaConnectorPluginVo {
    /// 插件类名（如 `io.debezium.connector.mysql.MySqlConnector`）
    pub class: String,
    /// 插件类型：`source` / `sink`
    ///
    /// 与 [`KafkaConnectorVo::connector_type`] 同义，只是描述的是「能装什么」而不是
    /// 「装了什么」：新建连接器前用它确认这台集群装了对应方向的 Debezium 插件。
    #[serde(rename = "type")]
    pub plugin_type: String,
    /// 插件版本
    #[serde(default)]
    pub version: Option<String>,
}

/// # 连接器 VO
///
/// 对应 `GET /connectors?expand=info&expand=status` 返回的单个连接器信息。
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct KafkaConnectorVo {
    /// 连接器名称
    pub name: String,
    /// 连接器类名（取自配置项 `connector.class`）
    #[serde(default)]
    pub connector_class: String,
    /// 连接器类型：`source` / `sink`
    ///
    /// 取自 Kafka Connect 的 `info.type`，是集群自己判定的方向，不是 dbz 的字段：
    /// `source` 表示数据从外部系统读进 Kafka（Debezium 上游），`sink` 表示从 Kafka
    /// 写到外部系统（下游）。用途是让运维在**未经 dbz 纳管**的连接器里也能分辨方向
    /// —— 例如发现一个 `source` 连接器却没有对应记录，说明有人在集群上手工建了一个
    /// 不受检查与告警覆盖的抓取任务。
    ///
    /// 集群版本过低、不支持 `expand=info` 时该字段为空串。
    #[serde(rename = "type", default)]
    pub connector_type: String,
    /// 连接器状态
    pub state: KafkaConnectorStatus,
    /// 任务列表
    #[serde(default)]
    pub tasks: Vec<KafkaConnectorTaskVo>,
}

/// # 连接器任务 VO
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct KafkaConnectorTaskVo {
    /// 任务ID（重启单个任务时使用）
    pub id: u32,
    /// 任务状态
    pub state: KafkaConnectorStatus,
    /// 执行该任务的 worker
    #[serde(default)]
    pub worker_id: Option<String>,
    /// 异常堆栈（任务失败时由 Kafka Connect 返回）
    #[serde(default)]
    pub trace: Option<String>,
}
