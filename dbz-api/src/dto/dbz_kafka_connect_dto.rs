//! # Kafka Connect DTO
//!
//! Kafka Connect 集群是连接器实际运行的位置。一台集群下可以挂多条上游连接器与
//! 多条下游连接器（各为 1:N），上游/下游只保存 `kafka_connect_id`；地址、连通
//! 状态与启用开关都收敛到本表，避免同一台集群被重复配置、重复探测。

use crate::dic::KafkaConnectStatus;
use robotech::macros::crud_dto;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

/// # Kafka Connect 健康检查 DTO
///
/// 只读接口：探测目标 Connect 是否可达，并带回版本、worker 等运行信息，
/// 不回写库内状态（回写由定时健康检查负责）。
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema, IntoParams)]
#[serde(default, rename_all = "camelCase")]
pub struct DbzKafkaConnectHealthDto {
    /// Kafka Connect ID
    pub kafka_connect_id: U64,
}

/// # Kafka Connect 检测 DTO
///
/// 写接口：以 Kafka Connect 上的实际连接器为准同步库内纳管记录，并回写
/// `inconsistent` 标记。只比对 `connectorConfig` 与远端是否存在，不看运行状态。
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema, IntoParams)]
#[serde(default, rename_all = "camelCase")]
pub struct DbzKafkaConnectDetectDto {
    /// Kafka Connect ID
    pub kafka_connect_id: U64,
}

#[crud_dto]
pub struct DbzKafkaConnectDto {
    /// 名称
    /// @len 1..=50
    pub name: String,
    /// 地址
    /// @len 1..=100
    pub url: String,
    /// 对应的 Kafka 集群
    ///
    /// 必填：没有 broker 地址就发不了 Kafka 信号，也查不了 topic。修改时不传表示不改挂。
    pub kafka_broker_id: U64,
    /// 状态
    #[db_default]
    pub run_status: KafkaConnectStatus,
    /// 检查结论（失败原因，由健康检查回写；成功时为空）
    ///
    /// 连不上时把探测到的原因（拒绝连接、超时、HTTP 4xx……）留在记录上，
    /// 排查时不必去翻日志。
    /// @len 0..=500
    pub run_msg: Option<String>,
    /// 备注
    /// @len 0..=50
    pub remark: Option<String>,
    /// 启用
    #[db_default]
    pub enabled: bool,
}
