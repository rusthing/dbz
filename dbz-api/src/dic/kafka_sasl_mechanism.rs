use robotech::macros::dic;
use strum::{AsRefStr, Display, EnumString};

/// SASL 认证机制，对应 `dbz_kafka_broker.sasl_mechanism`。
///
/// 只在 `security_protocol = SaslPlaintext` 时有意义，其余情况下必须是 `None`；
/// 这条约束由服务层在写入前校验，避免出现「协议是明文却配了机制」这种自相矛盾的记录。
#[dic(default = None)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr, Display, EnumString)]
#[strum(serialize_all = "kebab-case")]
pub enum KafkaSaslMechanism {
    /// 不使用（安全协议为明文时）
    None = 0,
    /// SASL/PLAIN
    Plain = 1,
    /// SCRAM-SHA-256
    ScramSha256 = 2,
    /// SCRAM-SHA-512
    ScramSha512 = 3,
}
