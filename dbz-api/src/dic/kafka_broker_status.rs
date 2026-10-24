use robotech::macros::dic;
use strum::{AsRefStr, Display, EnumString};

/// Kafka 集群记录的状态，对应 `dbz_kafka_broker.run_status`。
///
/// 与 [`KafkaConnectStatus`](crate::dic::KafkaConnectStatus) 一样只描述**集群本身**
/// 是否可达：能拉到 metadata 就是「正常」，连不上就是「连接失败」。连接器层面的
/// 故障属于 [`ConnectorRunStatus`](crate::dic::ConnectorRunStatus)。
///
/// 被 `DbzKafkaBrokerVo` 用作字段，所以必须声明 `default`。
#[dic(default = None)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr, Display, EnumString)]
#[strum(serialize_all = "kebab-case")]
pub enum KafkaBrokerStatus {
    /// 无状态（刚添加记录，或刚改过地址/认证/重新启用，等待下一轮检查）
    None = 0,
    /// 正常
    Normal = 1,
    /// 已禁用
    Disabled = 2,
    /// 连接失败
    ConnectFailed = 3,
}
