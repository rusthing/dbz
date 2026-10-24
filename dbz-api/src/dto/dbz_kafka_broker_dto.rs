//! # Kafka 集群（broker 侧）DTO
//!
//! `dbz_kafka_connect` 记的是 Connect 的 REST 地址，本表记的是**消息集群**的地址与
//! 认证。两者分开的理由见 `mo::dbz_kafka_broker` 的说明。
//!
//! `sasl_password` 只在写入时出现：查询结果（VO）不带密码，改配置时留空就表示
//! 「密码不变」，避免每次改备注都要把密码重新打一遍。

use crate::dic::{KafkaBrokerStatus, KafkaSaslMechanism, KafkaSecurityProtocol};
use robotech::macros::crud_dto;

#[crud_dto]
pub struct DbzKafkaBrokerDto {
    /// 名称
    /// @len 1..=50
    pub name: String,
    /// Broker 地址（`host:port`，多个用逗号分隔）
    /// @len 1..=500
    pub bootstrap_servers: String,
    /// 安全协议
    #[db_default]
    pub security_protocol: KafkaSecurityProtocol,
    /// SASL 机制（安全协议为明文时保持 `None`）
    #[db_default]
    pub sasl_mechanism: KafkaSaslMechanism,
    /// 认证用户名
    /// @len 0..=100
    pub sasl_username: Option<String>,
    /// 认证密码（留空表示不修改）
    /// @len 0..=200
    pub sasl_password: Option<String>,
    /// 状态
    #[db_default]
    pub run_status: KafkaBrokerStatus,
    /// 备注
    /// @len 0..=50
    pub remark: Option<String>,
    /// 启用
    #[db_default]
    pub enabled: bool,
}
