//! # 死信记录 DTO
//!
//! 死信不是通用 CRUD：写入来自 magic 脚本，读侧需要按连接器/状态/时间筛选，
//! 处理动作是重投与废弃，因此 DTO 按场景手写，不用 `#[crud_dto]`。

use crate::dic::{
    DeadLetterConnectorType, DeadLetterFailureCategory, DeadLetterFailureSubtype, DeadLetterStatus,
};
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
    /// 原始消息所在分区；脚本能拿到就传，列表据此展示来源
    pub partition: Option<i32>,
    /// 原始消息的 offset；脚本能拿到就传
    pub offset: Option<i64>,
    /// 原始报文（JSON）
    pub payload: serde_json::Value,
    /// 失败分类：确定性失败 / 传输类失败
    pub failure_category: DeadLetterFailureCategory,
    /// 失败子类：字段不合规时传 `1`，只有这类确定性失败允许修改报文后重投
    #[serde(default)]
    pub failure_subtype: DeadLetterFailureSubtype,
    /// 失败详情（HTTP 状态与响应体、校验报错原文、RTW 错误码、异常栈摘要等）
    /// 服务端原样保存，不做加工、不截断；给人看的原因由后端按分类生成
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
/// 单条重投就是这个接口只传一个 ID。逐条处理，返回每条的结果，部分失败不回滚。
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzDeadLetterRetryDto {
    /// 死信记录 ID
    /// @len 1..
    pub ids: Vec<U64>,
    /// 修改后的报文（JSON 文本）；不传则按原始报文重投。
    /// 仅单条重投可用，且记录必须是「确定性失败 + 字段不合规」。
    pub payload: Option<String>,
    /// 备注（可选）
    pub remark: Option<String>,
}

/// # 保存待投报文
///
/// 原始报文永远不覆盖，这里只保存修改后的待投报文与编辑审计字段；
/// 只有「确定性失败 + 字段不合规」的死信允许调用。
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
