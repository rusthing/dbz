//! # 控制面画布连线折点 VO

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// # 一条连线的折点
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzControlPlaneEdgeBendVo {
    /// 连线 ID
    pub edge_id: String,
    /// 正交走线中间水平段的世界坐标 Y
    pub mid_y: f64,
    /// 最后更新时间戳（毫秒）
    pub update_ms: i64,
}
