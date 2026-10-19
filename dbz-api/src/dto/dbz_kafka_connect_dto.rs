//! # Kafka Connect DTO
//!
//! Kafka Connect 集群是连接器实际运行的位置。一台集群下可以挂多条上游连接器与
//! 多条下游连接器（各为 1:N），上游/下游只保存 `kafka_connect_id`；地址、连通
//! 状态与启用开关都收敛到本表，避免同一台集群被重复配置、重复探测。

use crate::dic::KafkaConnectStatus;
use robotech::macros::crud_dto;

#[crud_dto]
pub struct DbzKafkaConnectDto {
    /// 名称
    pub name: String,
    /// 地址
    pub url: String,
    /// 状态
    #[db_default]
    pub run_status: KafkaConnectStatus,
    /// 备注
    pub remark: Option<String>,
    /// 启用
    #[db_default]
    pub enabled: bool,
}
