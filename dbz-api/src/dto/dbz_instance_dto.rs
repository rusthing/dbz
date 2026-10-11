use crate::dic::InstanceRunStatus;
use robotech::macros::crud_dto;

#[crud_dto]
pub struct DbzInstanceDto {
    /// 实例名称
    pub name: String,
    /// 关联的任务计划ID
    ///
    /// 由注册接口创建 jobx 定时检查任务后自动回填，调用方无需传入。
    pub job_id: u64,
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
    #[db_default]
    pub run_status: InstanceRunStatus,
    /// 备注
    pub remark: Option<String>,
    /// 启用
    #[db_default]
    pub enabled: bool,
}