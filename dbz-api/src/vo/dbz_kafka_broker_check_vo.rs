//! # Kafka 集群连通性检测结果
//!
//! 不落库、没有 `mo` 实体，因此不用 `#[vo]` 宏，手写 `Serialize` / `ToSchema`。
//!
//! 检测不只是「端口能连」：客户端会真的发一次 metadata 请求，能拿到 topic 列表才算
//! 通 —— 只连 TCP 的话，broker 在重启、SASL 认证配错、集群在选主的过程中都可能
//! 出现「端口开着但不干活」。

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// # 检测结果
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzKafkaBrokerCheckVo {
    /// 是否连通
    pub reachable: bool,
    /// 拉到的 topic 数量（不通时为 0）
    pub topic_count: u32,
    /// 耗时（毫秒）
    pub elapsed_ms: u64,
    /// 失败原因或提示；连通时通常是 `None`
    pub message: Option<String>,
}
