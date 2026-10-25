//! # 上游连接器 DTO
//!
//! 上游（source）连接器是 Debezium 从源库读取变更并写入 Kafka 的连接器。
//! 它通过 `kafka_connect_id` 指向运行它的 Kafka Connect 集群，地址由集群提供；
//! 其下挂的下游连接器见 `DbzSinkConnectorDto`。

use crate::dic::ConnectorRunStatus;
use robotech::macros::crud_dto;

#[crud_dto]
pub struct DbzSourceConnectorDto {
    /// Kafka Connect ID
    pub kafka_connect_id: u64,
    /// 名称
    /// @len 1..=50
    pub name: String,
    /// 连接器名称
    /// @len 1..=50
    pub connector_name: String,
    /// 连接器配置（JSON）
    /// @len 1..
    pub connector_config: String,
    /// 运行状态
    #[db_default]
    pub run_status: ConnectorRunStatus,
    /// 检查结论（失败原因，由健康检查回写；成功时为空）
    ///
    /// 只为了让检查把「为什么失败」留下来给人看，不参与业务判断。
    /// @len 0..=500
    pub run_msg: Option<String>,
    /// 备注
    /// @len 0..=50
    pub remark: Option<String>,
    /// 启用
    #[db_default]
    pub enabled: bool,
}
