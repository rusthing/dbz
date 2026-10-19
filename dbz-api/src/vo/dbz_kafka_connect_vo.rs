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
    /// 状态
    pub run_status: KafkaConnectStatus,
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
