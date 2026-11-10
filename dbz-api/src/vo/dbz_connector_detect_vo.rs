//! # 单条连接器一致性检测结果
//!
//! 与 `POST /dbz/kafka-connect/detect` 用的是同一套判定（`config_mismatch` /
//! `compare_detail_text`）：只比库内配置里出现过的键，敏感项脱敏，文本与数字
//! 能解析成同一数值时视为一致。检测会把结论回写库内 `inconsistent` 与
//! `inconsistent_detail`，前端刷新后即可点「不一致」标签逐项对比与双向同步。
//!
//! 这里没有 `mo` 实体，是纯计算结果，因此手写 `Serialize` / `ToSchema`。

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// # 单条连接器一致性检测结果
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzConnectorDetectVo {
    /// 节点类型：source / sink
    pub node_type: String,
    /// 连接器名（Connect 上使用的名称）
    pub connector_name: String,
    /// 库内 `connectorConfig` 是否为合法 JSON 对象
    pub config_valid: bool,
    /// 检测时 Connect 上是否还存在该连接器
    pub remote_exists: bool,
    /// 检测后库内的不一致标记
    pub inconsistent: bool,
    /// 检测后库内的脱敏对比快照（一致时为空）
    pub inconsistent_detail: Option<String>,
    /// 差异项数量
    pub changed: u32,
    /// 差异项键名（敏感项只给键名，不给值）
    pub changed_keys: Vec<String>,
    /// 无法逐项比对或远端不存在的原因
    pub reason: Option<String>,
    /// 本次检测时间（毫秒时间戳）
    pub checked_at: u64,
    /// 本次是否改动了库内标记或快照
    pub updated: bool,
}
