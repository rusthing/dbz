//! # 控制面操作 DTO
//!
//! 画布连线折点与节点删除。删除必须带当前登录用户口令的一次摘要，服务端转发给
//! adm-rs 的 `POST /adm/auth/verify-password` 二次确认后才真正删除；摘要本身不落库。

use robotech::api::U64;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

/// # 保存一条连线的折点
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzControlPlaneEdgeBendUpsertDto {
    /// 连线 ID
    /// @len 1..255
    pub edge_id: String,
    /// 正交走线中间水平段的世界坐标 Y
    pub mid_y: f64,
}

/// # 删除一条连线的折点（恢复默认居中走线）
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct DbzControlPlaneEdgeBendDeleteDto {
    /// 连线 ID
    pub edge_id: String,
}

/// # 删除一个画布节点
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzControlPlaneNodeDeleteDto {
    /// 节点类型：broker / connect / source / sink
    /// @len 1..32
    pub node_type: String,
    /// 节点记录 ID
    pub node_id: U64,
    /// 当前登录用户口令的 SM3 摘要（64 位小写十六进制）
    ///
    /// 由前端对明文做一次 SM3 后提交，明文与摘要都不会落库；
    /// 服务端只把它原样转发给 adm-rs 的 `POST /adm/auth/verify-password`。
    /// @len 64..64
    pub login_pswd: String,
}
