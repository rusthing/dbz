//! # 连接器视图对象
//!
//! 承载 Kafka Connect REST API 返回的连接器信息（插件、连接器及其任务），
//! 供 dbz 的运维接口直接返回给调用方。
//!
//! 注意：这些结构不落库、没有对应的 `mo` 实体，因此**不使用** `#[vo]` 宏
//! （该宏要求存在同名的 sea-orm 实体），而是手写 `ToSchema` / `Serialize`。
//! 字段命名统一为 `camelCase`，与 `#[vo]` 生成的 VO 保持一致。

use crate::dic::ConnectorStatus;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// # 连接器插件 VO
///
/// 对应 `GET /connector-plugins` 返回的单个插件信息。
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConnectorPluginVo {
    /// 插件类名（如 `io.debezium.connector.mysql.MySqlConnector`）
    pub class: String,
    /// 插件类型：`source` / `sink`
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
pub struct ConnectorVo {
    /// 连接器名称
    pub name: String,
    /// 连接器类名（取自配置项 `connector.class`）
    #[serde(default)]
    pub connector_class: String,
    /// 连接器类型：`source` / `sink`
    #[serde(rename = "type", default)]
    pub connector_type: String,
    /// 连接器状态
    pub state: ConnectorStatus,
    /// 任务列表
    #[serde(default)]
    pub tasks: Vec<ConnectorTaskVo>,
}

/// # 连接器任务 VO
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConnectorTaskVo {
    /// 任务ID（重启单个任务时使用）
    pub id: u32,
    /// 任务状态
    pub state: ConnectorStatus,
    /// 执行该任务的 worker
    #[serde(default)]
    pub worker_id: Option<String>,
    /// 异常堆栈（任务失败时由 Kafka Connect 返回）
    #[serde(default)]
    pub trace: Option<String>,
}
