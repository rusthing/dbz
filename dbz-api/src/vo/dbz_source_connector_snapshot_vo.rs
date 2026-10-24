//! # 上游连接器快照预检 VO
//!
//! 回答弹窗要显示的三件事：连的是哪个源库、信号往哪张表里插、这次会快照哪些表。
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
    /// 信号表全限定名（`signal.data.collection`）
    pub signal_collection: String,
    /// 本次会快照的表全限定名，按名称排序
    #[serde(default)]
    pub data_collections: Vec<String>,
    /// 解析过程中的提醒（如 `table.include.list` 有条目没匹配到表）
    #[serde(default)]
    pub warnings: Vec<String>,
}
