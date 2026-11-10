//! # 死信记录 VO
//!
//! `payload` 是原始报文，保持 JSON 原样；其余字段按行映射，枚举以整数输出。

use crate::dic::{
    DeadLetterConnectorType, DeadLetterFailureCategory, DeadLetterFailureSubtype, DeadLetterStatus,
};
use robotech::api::U64;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// # 死信记录
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzDeadLetterVo {
    /// 死信 ID
    pub id: U64,
    /// 连接器类型（DLQ 只对 sink 有效，恒为 2）
    pub connector_type: DeadLetterConnectorType,
    /// 下游连接器记录 ID（由 DLQ 头里的连接器名反查，未归属为 0）
    pub connector_id: U64,
    /// 原始消息所在的源 topic
    pub topic: String,
    /// DLQ 记录的 key（无 key 时空串）
    pub record_key: String,
    /// 原始消息所在分区；无 DLQ context headers 时用 DLQ 分区兜底
    pub partition: Option<i32>,
    /// 原始消息 offset；无 DLQ context headers 时用 DLQ offset 兜底
    pub offset: Option<i64>,
    /// DLQ 记录的原始报文（非 JSON 文本包成 `{"__raw_utf8": ...}`）
    #[schema(value_type = Object)]
    pub payload: serde_json::Value,
    /// 异常展示分类（由 DLQ 异常类名推导）
    pub failure_category: DeadLetterFailureCategory,
    /// 异常子类（字段不合规才允许改报文）
    pub failure_subtype: DeadLetterFailureSubtype,
    /// 是否允许修改报文后重投（服务端判定，前端只按此值给入口）
    pub can_edit_payload: bool,
    /// 修改后的待投报文；为空表示没有修改过，原始报文永不覆盖
    #[schema(value_type = Option<Object>)]
    pub pending_payload: Option<serde_json::Value>,
    /// 待投报文的最后编辑人
    pub payload_edited_by: Option<U64>,
    /// 待投报文的最后编辑时间戳（毫秒）
    pub payload_edited_ms: Option<i64>,
    /// 最近一次人工重投是否成功；为空表示还没有人工重投过
    pub last_retry_success: Option<bool>,
    /// 最近一次人工重投的时间戳（毫秒）
    pub last_retry_ms: Option<i64>,
    /// 最近一次人工重投的结果说明（写回源 topic 的结果）
    pub last_retry_message: Option<String>,
    /// 失败原因：由后端按 DLQ 异常分类生成的一句清晰说明
    pub reason: String,
    /// 失败详情：DLQ 上下文原文（异常类名、消息、堆栈与坐标），原样保存不加工
    pub failure_detail: String,
    /// 保留列：Connect 的重试次数不写进 DLQ，恒为 0
    pub retry_count: u32,
    /// Kafka 记录时间戳（毫秒）
    pub first_seen_ms: i64,
    /// Kafka 记录时间戳（毫秒），与 first_seen_ms 相同
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
