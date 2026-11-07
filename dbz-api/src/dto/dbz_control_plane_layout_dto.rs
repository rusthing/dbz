//! # 控制面画布布局 DTO
//!
//! 画布位置按「节点类型 + 节点 ID」逐条保存：一个人拖动节点只写自己那一行，
//! 多人同时整理不同节点不会互相覆盖。

use crate::dic::ControlPlaneNodeType;
use robotech::api::U64;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

/// # 保存一个节点的画布位置
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzControlPlaneLayoutUpsertDto {
    /// 节点类型
    pub node_type: ControlPlaneNodeType,
    /// 节点记录 ID
    pub node_id: U64,
    /// 画布世界坐标 X
    pub x: f64,
    /// 画布世界坐标 Y
    pub y: f64,
}

/// # 删除一个节点的布局记录
///
/// 节点被删除后清掉对应位置，避免留下永远对不上记录的空行。
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct DbzControlPlaneLayoutDeleteDto {
    /// 节点类型
    pub node_type: ControlPlaneNodeType,
    /// 节点记录 ID
    pub node_id: U64,
}
