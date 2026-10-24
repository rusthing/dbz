use robotech::macros::dic;
use strum::{AsRefStr, Display, EnumString};

/// Kafka 集群的安全协议，对应 `dbz_kafka_broker.security_protocol`。
///
/// 只列**已经能被 dbz 真正连上**的两种：dbz 用纯 Rust 客户端互通，TLS 需要额外
/// 的 CA 配置窗口，暂时不放出这两个取值 —— 与其让人在下拉里选中一个用不了的值，
/// 不如先只给能用的。
///
/// 被 `DbzKafkaBrokerVo` 用作字段，所以必须声明 `default`。
#[dic(default = Plaintext)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr, Display, EnumString)]
#[strum(serialize_all = "kebab-case")]
pub enum KafkaSecurityProtocol {
    /// 明文
    Plaintext = 0,
    /// SASL 明文（用户名/密码认证，机制见 `sasl_mechanism`）
    SaslPlaintext = 1,
}

impl KafkaSecurityProtocol {
    /// 是否需要用户名/密码
    pub fn needs_credentials(&self) -> bool {
        matches!(self, Self::SaslPlaintext)
    }
}
