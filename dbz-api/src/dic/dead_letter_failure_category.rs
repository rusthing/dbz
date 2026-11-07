use robotech::macros::dic;
use strum::{AsRefStr, Display, EnumString};

/// 失败分类，对应 `dbz_dead_letter.failure_category`。
///
/// 这是「为什么失败」的粗粒度区分，不是具体错误码：
/// - `Deterministic`：报文本身或数据本身有问题（参数校验不通过、唯一键冲突、
///   外键缺失），重投多少次结果都一样，直接进死信；
/// - `Transient`：对端此刻不可用（连接失败、5xx、超时、响应无法解析），已按
///   退避重试过，超过上限才进死信。
///
/// 具体错误放在 `failure_detail`，分类只用于筛选与统计。
#[dic(default = Deterministic)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr, Display, EnumString)]
#[strum(serialize_all = "kebab-case")]
pub enum DeadLetterFailureCategory {
    /// 确定性失败（数据/报文问题）
    Deterministic = 1,
    /// 传输类失败（对端不可用）
    Transient = 2,
    /// 响应异常（result 非成功且无法归类、响应无法解析）
    Unknown = 3,
}
