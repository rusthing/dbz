//! # 控制面画布布局 VO

use crate::dic::ControlPlaneNodeType;
use robotech::api::U64;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// # 一个节点的画布位置
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzControlPlaneLayoutVo {
    /// 节点类型
    pub node_type: ControlPlaneNodeType,
    /// 节点记录 ID
    pub node_id: U64,
    /// 画布世界坐标 X
    pub x: f64,
    /// 画布世界坐标 Y
    pub y: f64,
    /// 最后更新时间戳（毫秒）
    pub update_ms: i64,
}
