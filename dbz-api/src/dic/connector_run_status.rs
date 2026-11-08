use robotech::macros::dic;
use strum::{AsRefStr, Display, EnumString};

/// 连接器记录的**统一状态**，对应 `dbz_source_connector.run_status` /
/// `dbz_sink_connector.run_status` 两列（两表语义一致，共用一个枚举）。
///
/// 控制面只展示这一个状态，不再把「库内状态 / 实时状态」并列；「不一致」已改为
/// 独立的落库标记 `inconsistent`，只由 Connect 卡片「检测」按配置内容与存在性写入，
/// 与这里的运行状态互不干扰（避免状态抖动造成误报）。状态由后端统一推导并回写：
/// 既有健康检查任务，也有进入控制面时触发的一次刷新。远端集群上的原始状态
/// [`KafkaConnectorStatus`](crate::dic::KafkaConnectorStatus) 只作为推导输入，不下发到前端。
///
/// 被 `DbzSourceConnectorVo` / `DbzSinkConnectorVo` 用作字段，所以必须声明 `default`。
#[dic(default = Unknown)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr, Display, EnumString)]
#[strum(serialize_all = "kebab-case")]
pub enum ConnectorRunStatus {
    /// 未知：还没有任何检查结论
    Unknown = 0,
    /// 运行中：连接器与任务都在正常跑
    Running = 1,
    /// 已停用：库里 `enabled = false`，不发实时请求
    Disabled = 2,
    /// 失败：连接器失败、任务异常或作业销毁
    Failed = 3,
    /// 仅纳管未部署：库里启用，但 Connect 上不存在该连接器
    NotDeployed = 4,
    /// 不可达：Kafka Connect 本身连不上，保留最后已知状态与时间
    Unreachable = 5,
    /// 已停止：Connect 上的连接器为 PAUSED / STOPPED
    Stopped = 6,
}
