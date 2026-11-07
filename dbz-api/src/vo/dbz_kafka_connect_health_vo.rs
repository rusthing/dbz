//! # Kafka Connect 健康检查结果
//!
//! 只读探测结果，不落库、没有 `mo` 实体，因此不用 `#[vo]` 宏，手写
//! `Serialize` / `ToSchema`。
//!
//! 可达性以 `GET /connectors` 为准（能列出连接器才算通）；版本、commit 与
//! Kafka cluster id 来自 Connect 根路径 `GET /`。根路径取不到时不判失败，
//! 只把对应字段留空 —— 有的代理会拦根路径，但 `/connectors` 仍可用。

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// # Kafka Connect 健康检查结果
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzKafkaConnectHealthVo {
    /// 是否可达（能列出连接器）
    pub reachable: bool,
    /// Connect 版本（取自根路径，取不到时为空）
    pub version: Option<String>,
    /// Connect 的 commit 号（取自根路径，取不到时为空）
    pub commit: Option<String>,
    /// Connect 所属 Kafka 集群 ID（取自根路径，取不到时为空）
    pub kafka_cluster_id: Option<String>,
    /// 集群上的连接器数量
    pub connector_count: u32,
    /// worker 数量（按连接器与任务上报的 worker id 去重）
    pub worker_count: u32,
    /// worker id 列表
    pub worker_ids: Vec<String>,
    /// 本次探测耗时（毫秒）
    pub elapsed_ms: u64,
    /// 失败原因或提示；可达时通常是 `None`
    pub message: Option<String>,
}
