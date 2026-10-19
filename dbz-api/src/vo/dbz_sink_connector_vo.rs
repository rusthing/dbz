use crate::dic::ConnectorRunStatus;
use robotech::macros::vo;

#[vo]
pub struct DbzSinkConnectorVo {
    /// ID
    pub id: u64,
    /// 上游连接器ID
    pub source_connector_id: u64,
    /// 名称
    pub name: String,
    /// 连接器名称
    pub connector_name: String,
    /// 连接器配置（JSON）
    pub connector_config: String,
    /// 运行状态
    pub run_status: ConnectorRunStatus,
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
