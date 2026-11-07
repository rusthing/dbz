//! # 控制面重置位点结果 VO

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// # 单步执行结果
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzResetOffsetsStepVo {
    /// 步骤：stop / deleteOffsets / resume
    pub step: String,
    /// 是否成功
    pub ok: bool,
    /// 失败原因或补充说明
    pub message: Option<String>,
}

/// # 重置位点整体结果
///
/// 任一步失败立即停止，`failed_step` 指明停在哪一步；`connector_state` 是停下时
/// 连接器在 Kafka Connect 上的实际状态，前端据此提示用户。
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzResetConnectorOffsetsVo {
    /// 三步是否全部成功
    pub ok: bool,
    /// 失败的步骤；全部成功时为空
    pub failed_step: Option<String>,
    /// 给用户看的总结
    pub message: String,
    /// 执行完成/停下时连接器的实际状态
    pub connector_state: String,
    /// 每步结果，按执行顺序排列
    pub steps: Vec<DbzResetOffsetsStepVo>,
}
