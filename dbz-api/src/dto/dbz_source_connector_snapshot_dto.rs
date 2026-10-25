//! # 上游连接器「再同步」请求 DTO
//!
//! 「再同步」不改连接器配置，而是往**源库的信号表**插一行 `execute-snapshot`：
//! Debezium 的 source 信号通道读到这行后，对指定表再跑一次快照，把当前数据以
//! `op=r`（read）重新投递到同一个 topic。
//!
//! 因此这两个 DTO 只描述「对哪个连接器、哪些表、用哪种快照」。源库地址、账号、
//! 信号表名一概不在这里传 —— 全部从该连接器记录自己的 `connector_config` 里取，
//! 否则调用方就能让 dbz 去连任意数据库。

use robotech::api::U64;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

/// # 快照类型
///
/// 与 Debezium 信号里 `data.type` 的取值一一对应。只用于请求参数、不落库，所以
/// 不做成 `#[dic]`（那个宏是给库列用的整数枚举），线上形态就是小写字符串。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum SnapshotType {
    /// 增量快照：分块读表、与流式读取并行，不阻塞连接器
    ///
    /// 需要连接器配置了 `signal.data.collection`。表必须有主键，否则 Debezium
    /// 无法按主键分块，会跳过该表。
    #[default]
    Incremental,
    /// 阻塞快照：暂停流式读取，按初始快照那套流程重读，完成后恢复
    ///
    /// 只在较新的 Debezium 版本可用；早期版本只认 `incremental`。
    Blocking,
}

impl SnapshotType {
    /// 信号 `data.type` 的取值
    pub fn value(&self) -> &'static str {
        match self {
            Self::Incremental => "incremental",
            Self::Blocking => "blocking",
        }
    }
}

/// # 信号通道
///
/// Debezium 的 `execute-snapshot` 只能通过信号通道送达，dbz 支持其中两种：
///
/// * `source` —— 往源库的信号表插一行。不需要 Kafka，但要 dbz 能直连业务库，
///   `signal.data.collection` 也必须配好。
/// * `kafka` —— 往信号 topic 投一条消息。不碰业务库，但连接器要显式启用 kafka
///   通道（`signal.enabled.channels` 里带上 `kafka`），并且得有一个 broker 记录
///   用来投递。
///
/// 两者都是「把信号交给连接器自己消费」，语义等价，选哪个只取决于环境里哪条路通。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum SignalChannel {
    /// 源库信号表
    #[default]
    Source,
    /// Kafka 信号 topic
    Kafka,
}

impl SignalChannel {
    /// 通道名，对应 `signal.enabled.channels` 里的取值
    pub fn value(&self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Kafka => "kafka",
        }
    }
}

/// # 快照预检 DTO
///
/// 只解析连接器配置（source 通道还会读一次源库的 `information_schema`），不写任何
/// 东西：用来在弹窗里列出「这次会快照哪些表」，让人确认后再真正发信号。
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema, IntoParams)]
#[serde(default, rename_all = "camelCase")]
pub struct DbzSourceConnectorSnapshotPreviewDto {
    /// 上游连接器ID
    pub id: U64,
    /// 想看哪条通道的预检结果，留空按默认顺序挑一条可用的
    pub channel: Option<SignalChannel>,
}

/// # 触发快照 DTO
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzSourceConnectorSnapshotDto {
    /// 上游连接器ID
    pub id: U64,
    /// 要快照的表
    ///
    /// 留空表示连接器捕获范围内的**全部**表。两条通道的取值形态不一样：
    ///
    /// * `source` 通道要具体表名（`库.表` / `模式.表`），会拼进信号表的 INSERT，
    ///   只接受简单标识符；只写表名时 MySQL 自动补 `database.dbname`、PG 补模式。
    /// * `kafka` 通道按 Debezium 的语义当**正则**用（`(public)\..*` 这类），
    ///   预检返回的表范围可以原样提交回来。
    #[serde(default)]
    pub data_collections: Vec<String>,
    /// 快照类型，默认增量
    #[serde(default)]
    pub snapshot_type: SnapshotType,
    /// 用哪条信号通道，留空按默认顺序挑一条可用的
    #[serde(default)]
    pub channel: Option<SignalChannel>,
}
