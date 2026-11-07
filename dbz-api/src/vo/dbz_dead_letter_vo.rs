//! # 死信记录 VO
//!
//! `payload` 是原始报文，保持 JSON 原样；其余字段按行映射，枚举以整数输出。

use crate::dic::{DeadLetterConnectorType, DeadLetterFailureCategory, DeadLetterStatus};
use robotech::api::U64;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// # 死信记录
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzDeadLetterVo {
    /// 死信 ID
    pub id: U64,
    /// 连接器类型
    pub connector_type: DeadLetterConnectorType,
    /// 连接器记录 ID
    pub connector_id: U64,
    /// Kafka topic
    pub topic: String,
    /// 记录主键
    pub record_key: String,
    /// 原始报文
    #[schema(value_type = Object)]
    pub payload: serde_json::Value,
    /// 失败分类
    pub failure_category: DeadLetterFailureCategory,
    /// 失败详情
    pub failure_detail: String,
    /// 累计失败次数
    pub retry_count: u32,
    /// 首次失败时间戳（毫秒）
    pub first_seen_ms: i64,
    /// 末次失败时间戳（毫秒）
    pub last_seen_ms: i64,
    /// 处理状态
    pub status: DeadLetterStatus,
    /// 处理人
    pub handled_by: Option<U64>,
    /// 处理时间戳（毫秒）
    pub handled_ms: Option<i64>,
    /// 备注
    pub remark: Option<String>,
    /// 创建时间戳（毫秒）
    pub create_ms: i64,
    /// 更新时间戳（毫秒）
    pub update_ms: i64,
}

/// # 单条重投结果
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzDeadLetterRetryResultVo {
    /// 死信 ID
    pub id: U64,
    /// 是否重投成功（成功才转「已重投」）
    pub success: bool,
    /// 失败原因或成功提示
    pub message: String,
}

/// # 待处理计数
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzDeadLetterCountVo {
    /// 连接器类型
    pub connector_type: DeadLetterConnectorType,
    /// 连接器记录 ID
    pub connector_id: U64,
    /// 待处理条数
    pub count: u64,
}
