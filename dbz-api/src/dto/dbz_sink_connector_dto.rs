//! # 下游连接器 DTO
//!
//! 下游（sink）连接器把 Kafka 中的变更写入目标存储。它通过 `source_connector_id`
//! 归属于某个上游连接器（一条上游可挂多条下游），自身不保存 Kafka Connect 地址 ——
//! 沿用上游连接器配置的地址，即两者必须注册在同一个 Kafka Connect 上。

use crate::dic::ConnectorRunStatus;
use robotech::macros::crud_dto;

#[crud_dto]
pub struct DbzSinkConnectorDto {
    /// 上游连接器ID
    pub source_connector_id: u64,
    /// 名称
    pub name: String,
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
