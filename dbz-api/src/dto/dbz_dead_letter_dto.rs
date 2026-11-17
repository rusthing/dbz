//! # 死信记录 DTO
//!
//! 死信不是通用 CRUD：写入来自 Kafka Connect 的 DLQ topic（服务端主动同步，没有
//! 上报接口），读侧需要按连接器/状态/时间筛选，处理动作是重投与废弃，因此 DTO
//! 按场景手写，不用 `#[crud_dto]`。

use crate::dic::{
    DeadLetterConnectorType, DeadLetterFailureCategory, DeadLetterFailureSubtype, DeadLetterStatus,
};
use robotech::api::U64;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

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
    /// 失败分类（不传表示不限）
    pub failure_category: Option<DeadLetterFailureCategory>,
    /// 失败子类（不传表示不限）
    pub failure_subtype: Option<DeadLetterFailureSubtype>,
    /// 失败原因关键字（对后端生成的 `reason` 做包含匹配）
    pub reason: Option<String>,
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
/// 把报文写回源 topic 交给 Kafka Connect 重新消费；单条重投就是只传一个 ID。
/// 逐条处理，返回每条的结果，部分失败不回滚。
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzDeadLetterRetryDto {
    /// 死信记录 ID
    /// @len 1..
    pub ids: Vec<U64>,
    /// 修改后的报文（JSON 文本）；不传则按 DLQ 里的原始报文重投。
    /// 仅单条重投可用，且记录必须是「数据/格式类 + 字段不合规」。
    pub payload: Option<String>,
    /// 备注（可选）
    pub remark: Option<String>,
}

/// # 保存待投报文
///
/// 原始报文永远不覆盖，这里只保存修改后的待投报文与编辑审计字段；
/// 只有「数据/格式类 + 字段不合规」的死信允许调用。重投时报文写回源 topic。
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzDeadLetterEditPayloadDto {
    /// 死信记录 ID
    pub id: U64,
    /// 修改后的报文（JSON 文本），服务端会校验合法性
    /// @len 1..
    pub payload: String,
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

/// # 批量标记「已人工处理」
///
/// 表示这条记录已经在别处修好，不再需要重投；处理说明写入备注，处理人与时间
/// 由服务端记录。与「已废弃」的区别是：前者是「问题已解决」，后者是「不再处理」。
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzDeadLetterMarkHandledDto {
    /// 死信记录 ID
    /// @len 1..
    pub ids: Vec<U64>,
    /// 处理说明（怎么处理的、在哪修的）
    pub remark: Option<String>,
}

/// # 批量撤销「已人工处理」
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzDeadLetterUnmarkHandledDto {
    /// 死信记录 ID
    /// @len 1..
    pub ids: Vec<U64>,
}

/// # 清理死信
///
/// 与「废弃」的区别：废弃只改状态、保留追溯；清理是**物理删除**页面索引，不可恢复。
/// 筛选条件全部不传等于清空整张索引表。`clearTopic=true` 时还会把 DLQ topic 里的
/// 原始记录一并删掉（Kafka `DeleteRecords` 把各分区的起始位点推到末尾）。
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct DbzDeadLetterPurgeDto {
    /// 连接器类型（不传表示不限）
    pub connector_type: Option<DeadLetterConnectorType>,
    /// 连接器 ID（不传表示不限）
    pub connector_id: Option<U64>,
    /// 处理状态（不传表示不限）
    pub status: Option<DeadLetterStatus>,
    /// 只清「末次失败时间」早于该毫秒时刻的记录（不传表示不限）
    pub before_ms: Option<i64>,
    /// 是否同时清空 DLQ topic 里的原始记录
    ///
    /// 默认否：只删 dbz 的页面索引，Kafka 里的报文由 retention 管。置 true 会调用
    /// Kafka 的 DeleteRecords 物理删除历史报文，不可恢复；topic 与分区保留，不会
    /// 触发 Connect 重建 topic，也不会让下游连接器重扫。
    pub clear_topic: bool,
}
