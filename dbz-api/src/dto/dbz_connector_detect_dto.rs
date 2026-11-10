//! # 单条连接器一致性检测 DTO
//!
//! 控制面板里上游 / 下游连接器卡片上的「检测」：只针对这一条纳管记录，
//! 按 `nodeType` + `nodeId` 定位，不牵连同一台 Connect 上的其它连接器
//! （那是 `POST /dbz/kafka-connect/detect` 的职责）。

use robotech::api::U64;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// # 检测一条连接器的配置一致性
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct DbzConnectorDetectDto {
    /// 节点类型：source / sink
    /// @len 1..32
    pub node_type: String,
    /// 节点记录 ID
    pub node_id: U64,
}
