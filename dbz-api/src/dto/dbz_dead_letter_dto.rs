//! # 死信记录 DTO
//!
//! 死信不是通用 CRUD：写入来自 magic 脚本，读侧需要按连接器/状态/时间筛选，
//! 处理动作是重投与废弃，因此 DTO 按场景手写，不用 `#[crud_dto]`。

use crate::dic::{DeadLetterConnectorType, DeadLetterFailureCategory, DeadLetterStatus};
use robotech::api::U64;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

/// # 上报一条死信（内部写入）
///
/// magic 脚本在失败分支调用：确定性失败直接上报；传输类失败按退避重试到上限后
/// 再上报。服务端按 `(connector_type, connector_id, topic, record_key)` 幂等：
/// 已存在时累加失败次数、刷新末次时间并重置为「待处理」，不新增行。
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzDeadLetterReportDto {
    /// 连接器类型（上游/下游）
    pub connector_type: DeadLetterConnectorType,
    /// 连接器记录 ID
    pub connector_id: U64,
    /// Kafka topic
    /// @len 1..
    pub topic: String,
    /// 记录主键；无主键时传空串
    #[serde(default)]
    pub record_key: String,
    /// 原始报文（JSON）
    pub payload: serde_json::Value,
    /// 失败分类：确定性失败 / 传输类失败
    pub failure_category: DeadLetterFailureCategory,
    /// 失败详情（对端的原始报错）
    #[serde(default)]
    pub failure_detail: String,
    /// 本次失败事件已经重试的次数；确定性失败传 0
    #[serde(default)]
    pub retry_count: u32,
}

/// # 死信分页查询
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema, IntoParams)]
#[serde(default, rename_all = "camelCase")]
pub struct DbzDeadLetterQueryDto {
    /// 连接器类型（不传表示不限）
    pub connector_type: Option<DeadLetterConnectorType>,
    /// 连接器 ID（不传表示不限）
    pub connector_id: Option<U64>,
    /// 处理状态（不传表示不限；角标只用 Pending）
    pub status: Option<DeadLetterStatus>,
    /// topic 精确匹配
    pub topic: Option<String>,
    /// 末次失败时间下界（毫秒，含）
    pub start_ms: Option<i64>,
    /// 末次失败时间上界（毫秒，含）
    pub end_ms: Option<i64>,
    /// 页码，从 1 开始
    #[serde(rename = "_page")]
    pub _page: Option<U64>,
    /// 每页条数
    #[serde(rename = "_size")]
    pub _size: Option<U64>,
}

/// # 待处理计数查询
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema, IntoParams)]
#[serde(default, rename_all = "camelCase")]
pub struct DbzDeadLetterCountDto {
    /// 连接器类型（不传表示全部）
    pub connector_type: Option<DeadLetterConnectorType>,
    /// 连接器 ID（不传表示按连接器分组返回全部）
    pub connector_id: Option<U64>,
}

/// # 批量重投
///
/// 单条重投就是这个接口只传一个 ID。逐条处理，返回每条的结果，部分失败不回滚。
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzDeadLetterRetryDto {
    /// 死信记录 ID
    /// @len 1..
    pub ids: Vec<U64>,
    /// 备注（可选）
    pub remark: Option<String>,
}

/// # 批量废弃
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzDeadLetterDiscardDto {
    /// 死信记录 ID
    /// @len 1..
    pub ids: Vec<U64>,
    /// 废弃原因
    pub remark: Option<String>,
}
