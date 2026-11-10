use robotech::macros::dic;
use strum::{AsRefStr, Display, EnumString};

/// 产生死信的连接器类型，对应 `dbz_dead_letter.connector_type`。
///
/// Kafka Connect 的 DLQ 只对 **sink** 连接器有效，所以新写入的记录恒为 `Sink`；
/// 保留 `Source` 是为了兼容旧字典取值与历史深链，服务端不再产生上游死信。
#[dic(default = Sink)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr, Display, EnumString)]
#[strum(serialize_all = "kebab-case")]
pub enum DeadLetterConnectorType {
    /// 上游（source）连接器
    Source = 1,
    /// 下游（sink）连接器
    Sink = 2,
}
