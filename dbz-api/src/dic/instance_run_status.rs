use robotech::macros::dic;
use strum::{AsRefStr, Display, EnumString};

/// 被 `DbzInstanceVo` 用作字段，所以必须声明 `default`。
#[dic(default = None)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr, Display, EnumString)]
#[strum(serialize_all = "kebab-case")]
pub enum InstanceRunStatus {
    /// 无状态（刚添加记录）
    None = 0,
    /// 运行中
    Running = 1,
    /// 已禁用
    Disabled = 2,
    /// 连接 KafkaConnect 失败
    KafkaConnectFailed = 3,
    /// 注册下游连接器失败
    RegisterSinkFailed = 4,
    /// 检测下游连接器失败
    CheckSinkFailed = 5,
    /// 检测下游连接器异常
    CheckSinkError = 6,
    /// 检测下游连接器状态失败
    CheckSinkStatusFailed = 7,
    /// 检测下游连接器状态异常
    CheckSinkStatusError = 8,
    /// 注册上游连接器失败
    RegisterSourceFailed = 9,
    /// 检测上游连接器失败
    CheckSourceFailed = 10,
    /// 检测上游连接器异常
    CheckSourceError = 11,
    /// 检测上游连接器状态失败
    CheckSourceStatusFailed = 12,
    /// 检测上游连接器状态异常
    CheckSourceStatusError = 13,
}
