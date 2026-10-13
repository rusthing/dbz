use crate::dic::InstanceRunStatus;
use robotech::macros::vo;

#[vo]
pub struct DbzInstanceVo {
    /// ID
    pub id: u64,
    /// 实例名称
    pub name: String,
    /// Kafka Connect 地址
    pub kafka_connect_url: String,
    /// 上游连接器名称
    pub source_connector_name: String,
    /// 上游连接器配置（JSON）
    pub source_connector_config: String,
    /// 下游连接器名称
    pub sink_connector_name: String,
    /// 下游连接器配置（JSON）
    pub sink_connector_config: String,
    /// 运行状态
    pub run_status: InstanceRunStatus,
    /// 备注
    pub remark: Option<String>,
    /// 启用
    pub enabled: bool,
    /// 创建者ID
    pub creater_id: u64,
    /// 创建时间
    pub create_ms: u64,
    /// 更新者ID
    pub updater_id: u64,
    /// 更新时间
    pub update_ms: u64,
}