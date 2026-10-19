use robotech::macros::dic;
use strum::{AsRefStr, Display, EnumString};

/// 连接器记录的运行状态，对应 `dbz_source_connector.run_status` /
/// `dbz_sink_connector.run_status` 两列（两表语义一致，故共用一个枚举）。
///
/// 与 [`ConnectorStatus`](crate::dic::ConnectorStatus) 的区别：`ConnectorStatus` 是
/// Kafka Connect REST API 返回的**实时**状态，不落库；本枚举是检查任务得出的**结论**，
/// 会回写到数据库。
///
/// 被 `DbzSourceConnectorVo` / `DbzSinkConnectorVo` 用作字段，所以必须声明 `default`。
#[dic(default = None)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr, Display, EnumString)]
#[strum(serialize_all = "kebab-case")]
pub enum ConnectorRunStatus {
    /// 无状态（刚添加记录）
    None = 0,
    /// 运行中
    Running = 1,
    /// 已禁用
    Disabled = 2,
    /// 连接 KafkaConnect 失败
    KafkaConnectFailed = 3,
    /// 注册连接器失败
    RegisterFailed = 4,
    /// 检测连接器失败
    CheckFailed = 5,
    /// 检测连接器异常
    CheckError = 6,
    /// 检测连接器状态失败
    CheckStatusFailed = 7,
    /// 检测连接器状态异常
    CheckStatusError = 8,
}
