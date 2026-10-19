use robotech::macros::dic;
use strum::{AsRefStr, Display, EnumString};

/// Kafka Connect 集群记录的状态，对应 `dbz_kafka_connect.run_status`。
///
/// 与 [`ConnectorRunStatus`](crate::dic::ConnectorRunStatus) 的区别：后者描述
/// **连接器**（注册是否成功、任务是否正常），本枚举只描述**集群本身**是否可达，
/// 所以没有「注册连接器失败」「检测连接器异常」那几档取值。
///
/// 被 `DbzKafkaConnectVo` 用作字段，所以必须声明 `default`。
#[dic(default = None)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr, Display, EnumString)]
#[strum(serialize_all = "kebab-case")]
pub enum KafkaConnectStatus {
    /// 无状态（刚添加记录，或刚改过地址/重新启用，等待下一轮检查）
    None = 0,
    /// 正常
    Normal = 1,
    /// 已禁用
    Disabled = 2,
    /// 连接失败
    ConnectFailed = 3,
}
