use robotech::macros::dic;
use strum::{AsRefStr, Display, EnumString};

/// Kafka 连接器在集群上的**实时**状态，取自 Kafka Connect REST API，不落库。
///
/// 三个状态枚举容易混，按「谁的事实」区分：
/// - `KafkaConnectorStatus`（本枚举）—— 远端连接器此刻的状态（RUNNING / FAILED / …）；
/// - [`ConnectorRunStatus`](crate::dic::ConnectorRunStatus) —— 本库记录 `run_status`
///   字段的取值，是检查任务得出的**结论**，会回写库；
/// - [`KafkaConnectStatus`](crate::dic::KafkaConnectStatus) —— 集群自身是否可达。
///
/// 默认变体是 `Unassigned`（未分配）—— 这是**业务语义**上的默认值
/// （连接器状态未知时按「未分配」处理，见 `kafka_connector_utils` 的兜底分支），
/// 而不是「第一个变体」，所以显式声明 `default`。
#[dic(default = Unassigned)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr, Display, EnumString)]
#[strum(serialize_all = "kebab-case")]
pub enum KafkaConnectorStatus {
    /// 运行中
    Running = 0,
    /// 已失败
    Failed = 1,
    /// 已暂停
    Paused = 2,
    /// 未分配
    Unassigned = 3,
    /// 已销毁
    Destroyed = 4,
}
