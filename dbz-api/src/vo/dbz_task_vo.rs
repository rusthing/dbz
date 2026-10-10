use crate::dic::{ConnectorStatus, ConnectorType};
use robotech::macros::vo;

#[vo]
pub struct DbzTaskVo {
    /// ID
    pub id: u64,
    /// 任务名称
    pub name: String,
    /// Kafka Connect 地址
    pub kafka_connect_url: String,
    /// 连接器名称
    pub connector_name: String,
    /// 连接器配置（JSON）
    pub connector_config: Option<String>,
    /// 连接器类型
    pub connector_type: ConnectorType,
    /// 连接器状态
    pub status: ConnectorStatus,
    /// 最后检查时间
    pub last_check_ms: Option<u64>,
    /// 最后状态详情
    pub last_status: Option<String>,
    /// 状态检查间隔（毫秒）
    pub check_interval: u64,
    /// 备注
    pub remark: Option<String>,
    /// 关联的任务计划ID
    pub job_id: Option<u64>,
    /// 启用
    pub enabled: bool,
    /// 创建者ID
    pub creator_id: u64,
    /// 创建时间
    pub create_ms: u64,
    /// 更新者ID
    pub updator_id: u64,
    /// 更新时间
    pub update_ms: u64,
}