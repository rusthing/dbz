use robotech::macros::dic;
use strum::{AsRefStr, Display, EnumString};

/// 异常分类，对应 `dbz_dead_letter.failure_category`。
///
/// 由 DLQ 记录头 `__connect.errors.exception.class.name` 与异常消息推导，**只用于
/// 页面筛选、一句话说明与「能否改报文」白名单**，不参与任何重试决策 —— 重试由
/// Kafka Connect 的 `errors.retry.*` 与连接器自己的请求级重试参数决定。
///
/// - `Deterministic`：数据/格式/配置类异常（DataException、序列化失败等），重投
///   结果通常不变；
/// - `Transient`：投递类异常（RetriableException、超时、IO、HTTP 错误），已由
///   Connect 重试到上限；
/// - `Unknown`：其它无法归类的异常。
///
/// 原始异常类名、消息与堆栈放在 `failure_detail`。
#[dic(default = Deterministic)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr, Display, EnumString)]
#[strum(serialize_all = "kebab-case")]
pub enum DeadLetterFailureCategory {
    /// 数据/格式类（DataException、序列化失败等报文或配置问题）
    Deterministic = 1,
    /// 投递类（RetriableException、超时、IO、HTTP 错误，Connect 已重试到上限）
    Transient = 2,
    /// 其它无法归类的异常
    Unknown = 3,
}
