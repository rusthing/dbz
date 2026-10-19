//! # 上游连接器 DTO
//!
//! 上游（source）连接器是 Debezium 从源库读取变更并写入 Kafka 的连接器。
//! 它自己保存 Kafka Connect 地址，其下挂的下游连接器沿用该地址
//! （见 `DbzSinkConnectorDto`）。

use crate::dic::ConnectorRunStatus;
use robotech::macros::crud_dto;

#[crud_dto]
pub struct DbzSourceConnectorDto {
    /// 名称
    pub name: String,
    /// Kafka Connect 地址
    pub kafka_connect_url: String,
    /// 连接器名称
    pub connector_name: String,
    /// 连接器配置（JSON）
    pub connector_config: String,
    /// 运行状态
    #[db_default]
    pub run_status: ConnectorRunStatus,
    /// 备注
    pub remark: Option<String>,
    /// 启用
    #[db_default]
    pub enabled: bool,
}
