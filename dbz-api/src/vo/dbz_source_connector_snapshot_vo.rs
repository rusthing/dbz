//! # 上游连接器快照预检 VO
//!
//! 回答弹窗要显示的四件事：连的是哪个源库、信号往哪走（信号表 / 信号 topic）、
//! 这次会快照哪些表、以及连接器上哪条通道可用。
//!
//! 与 `kafka_connector_vo` 一样不落库、没有 `mo` 实体，因此不用 `#[vo]` 宏，
//! 手写 `Serialize` / `ToSchema`，字段名同样是 `camelCase`。

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// # 快照预检结果
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzSourceConnectorSnapshotVo {
    /// 源库方言：`mysql` / `postgresql`
    pub dialect: String,
    /// 连接器连的那个库（`database.dbname`）
    pub database: String,
    /// 这台连接器上可用的信号通道，按推荐顺序排列（`source` / `kafka`）
    ///
    /// 与 `channel` 无关：切换通道后这份清单不变，前端才能一直画出两个切换项。
    #[serde(default)]
    pub channels: Vec<String>,
    /// 本次预检实际用的通道
    pub channel: String,
    /// 信号表全限定名（`source` 通道）
    pub signal_collection: Option<String>,
    /// 信号 topic（`kafka` 通道，`signal.kafka.topic`）
    pub kafka_topic: Option<String>,
    /// 投递信号用的 Kafka 集群名（`kafka` 通道）
    pub kafka_broker: Option<String>,
    /// 本次会快照的表：`source` 通道是具体表名，`kafka` 通道是交给连接器展开的正则
    #[serde(default)]
    pub data_collections: Vec<String>,
    /// 解析过程中的提醒（如 `table.include.list` 有条目没匹配到表）
    #[serde(default)]
    pub warnings: Vec<String>,
}
