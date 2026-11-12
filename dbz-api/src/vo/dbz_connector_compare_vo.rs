//! # 连接器配置对比快照
//!
//! 检测时把库内配置与远端 Connect 实际配置逐项落库的脱敏快照，供前端「不一致」
//! 标签点开查看。字段与判定同源：两侧的键集合都参与（都排除 `name`），与
//! `config_mismatch` 的口径完全一致。
//!
//! 只在远端出现的键同样进快照：它是「库内配置重建不出这个连接器」的直接证据
//! （有人在 Connect 上直接加过配置，或者插件升级改了键名），只展示库内键会让人
//! 把「远端已有等价的新键」误读成「远端少配了几项」。
//!
//! 敏感项（password / secret / token / sasl.jaas.config 等）不保存明文，值统一
//! 替换为「已脱敏」；若该项正好是差异项，再标注「已变更」，保证既不复述密钥，
//! 又不会出现「标签说不一致、弹窗却看不出差别」。

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// # 单行配置对比
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzConnectorCompareRow {
    /// 配置项键名
    pub key: String,
    /// 库内配置值（敏感项已脱敏；库内没有这一项时为空）
    pub local: Option<String>,
    /// 远端 Connect 实际值（敏感项已脱敏；远端缺少该键时为空）
    pub remote: Option<String>,
    /// 是否参与判定为「不一致」，与检测结果同源
    pub changed: bool,
    /// 该项是否为敏感字段（值已脱敏）
    pub sensitive: bool,
    /// 是否只在远端存在（库内没有纳管这一项）
    ///
    /// 这类项只能整体同步、不能在弹窗里逐项编辑；老快照没有该字段，按 `false` 兜底。
    #[serde(default)]
    pub remote_only: bool,
}

/// # 连接器配置对比快照
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzConnectorCompareDetail {
    /// 快照生成时间（毫秒时间戳）
    pub checked_at: u64,
    /// 检测时远端 Connect 上是否还存在该连接器
    pub remote_exists: bool,
    /// 无法逐项展示时的原因（如配置不是合法 JSON 对象）
    pub reason: Option<String>,
    /// 逐项对比，按配置项键名排序，顺序稳定
    pub rows: Vec<DbzConnectorCompareRow>,
}

impl From<String> for DbzConnectorCompareDetail {
    fn from(value: String) -> Self {
        serde_json::from_str(&value).unwrap_or_default()
    }
}
