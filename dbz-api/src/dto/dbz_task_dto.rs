use crate::dic::{ConnectorStatus, ConnectorType};
use robotech::macros::crud_dto;

#[crud_dto]
pub struct DbzTaskDto {
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
    #[db_default]
    pub status: ConnectorStatus,
    /// 最后检查时间（毫秒时间戳）
    pub last_check_ms: Option<i64>,
    /// 最后状态详情
    pub last_status: Option<String>,
    /// 状态检查间隔（毫秒）
    pub check_interval: i64,
    /// 备注
    pub remark: Option<String>,
    /// 关联的任务计划ID（用于定时检查状态）
    pub job_id: Option<i64>,
    /// 启用
    #[db_default]
    pub enabled: bool,
}