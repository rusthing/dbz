//! # Kafka Connect 检测结果
//!
//! 检测以 Connect 上的实际连接器为准同步库内纳管记录：
//! 远端有、库内没有的自动补录；配置不一致的写 `inconsistent=true`；
//! 远端已不存在的置为禁用并标记。只比对 `connectorConfig` 与存在性，
//! 不看运行状态，避免状态抖动造成误报。

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// # 单条连接器的检测结论
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzConnectorDetectItemVo {
    /// 连接器名（Connect 上使用的名称）
    pub connector_name: String,
    /// `source` / `sink` / `unknown`
    pub connector_type: String,
    /// `added` 补录 / `marked` 标记不一致 / `disabled` 置禁用并标记 /
    /// `cleared` 恢复一致 / `unchanged` 无变化 / `skipped` 无法自动处理
    pub action: String,
    /// 补充说明（跳过的原因等）
    pub message: Option<String>,
}

/// # Kafka Connect 检测结果
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzKafkaConnectDetectVo {
    /// 远端有、库内没有，已自动补录
    pub added: u32,
    /// 配置不一致，已标记
    pub marked: u32,
    /// 远端已不存在，已置为禁用并标记
    pub disabled: u32,
    /// 原来标记不一致，本次检测已恢复一致
    pub cleared: u32,
    /// 一致且无变化
    pub unchanged: u32,
    /// 无法自动处理（下游找不到上游、低版本取不到类型、配置读取失败等）
    pub skipped: u32,
    /// 明细
    pub items: Vec<DbzConnectorDetectItemVo>,
}
