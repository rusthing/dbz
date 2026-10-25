use crate::dic::{KafkaBrokerStatus, KafkaSaslMechanism, KafkaSecurityProtocol};
use robotech::macros::vo;

/// # Kafka 集群（broker 侧）
///
/// 不返回 `sasl_password`：密码只进不出，改配置时留空即表示不变。前端也就不会
/// 把密码渲染到页面上。
#[vo]
pub struct DbzKafkaBrokerVo {
    /// ID
    pub id: u64,
    /// 名称
    pub name: String,
    /// Broker 地址（`host:port`，多个用逗号分隔）
    pub bootstrap_servers: String,
    /// 安全协议
    pub security_protocol: KafkaSecurityProtocol,
    /// SASL 机制
    pub sasl_mechanism: KafkaSaslMechanism,
    /// 认证用户名
    pub sasl_username: Option<String>,
    /// 状态
    pub run_status: KafkaBrokerStatus,
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
