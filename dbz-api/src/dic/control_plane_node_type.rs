use robotech::macros::dic;
use strum::{AsRefStr, Display, EnumString};

/// 控制面画布上的节点类型，对应 `dbz_control_plane_layout.node_type`。
///
/// 四类节点的记录分散在四张表里，位置表需要用类型区分同一个 ID 落在哪张表。
#[dic(default = Broker)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr, Display, EnumString)]
#[strum(serialize_all = "kebab-case")]
pub enum ControlPlaneNodeType {
    /// Kafka 集群
    Broker = 1,
    /// Kafka Connect
    Connect = 2,
    /// 上游（source）连接器
    Source = 3,
    /// 下游（sink）连接器
    Sink = 4,
}
