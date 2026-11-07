use robotech::macros::dic;
use strum::{AsRefStr, Display, EnumString};

/// 产生死信的连接器类型，对应 `dbz_dead_letter.connector_type`。
///
/// 上游与下游连接器的记录在两张表里，ID 全局唯一但不带类型就无法确定该查哪张表，
/// 因此死信表把类型单独存一列。
#[dic(default = Sink)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr, Display, EnumString)]
#[strum(serialize_all = "kebab-case")]
pub enum DeadLetterConnectorType {
    /// 上游（source）连接器
    Source = 1,
    /// 下游（sink）连接器
    Sink = 2,
}
