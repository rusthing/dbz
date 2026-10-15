use robotech::macros::dic;
use strum::{AsRefStr, Display, EnumString};

/// 默认变体是 `Unassigned`（未分配）—— 这是**业务语义**上的默认值
/// （连接器状态未知时按「未分配」处理，见 `dbz_connector_utils` 的兜底分支），
/// 而不是「第一个变体」，所以显式声明 `default`。
#[dic(default = Unassigned)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr, Display, EnumString)]
#[strum(serialize_all = "kebab-case")]
pub enum ConnectorStatus {
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
