use crate::dic::KafkaConnectStatus;
use robotech::macros::vo;

#[vo]
pub struct DbzKafkaConnectVo {
    /// ID
    pub id: u64,
    /// 名称
    pub name: String,
    /// 地址
    pub url: String,
    /// 对应的 Kafka 集群（老记录可能为空）
    pub kafka_broker_id: Option<U64>,
    /// 状态
    pub run_status: KafkaConnectStatus,
    /// 检查结论（最近一次检查的失败原因，成功时为空）
    pub run_msg: Option<String>,
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
