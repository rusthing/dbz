//! # 控制面重置位点 DTO

use crate::dic::ControlPlaneNodeType;
use robotech::api::U64;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// # 重置连接器位点
///
/// 后端按 Kafka Connect 的约束串行执行「停止 → 删除位点 → 可选恢复」。下游连接器
/// 要求显式确认会重放历史消息，避免误操作造成重复写入。
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzResetConnectorOffsetsDto {
    /// 节点类型，只接受 Source / Sink
    pub connector_type: ControlPlaneNodeType,
    /// 连接器记录 ID
    pub connector_id: U64,
    /// 清空后是否立即恢复运行
    #[serde(default)]
    pub resume: bool,
    /// 下游连接器必须显式确认「会重放历史消息、可能重复写入」
    #[serde(default)]
    pub confirm_replay: bool,
}
