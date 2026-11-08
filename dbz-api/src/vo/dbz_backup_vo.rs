//! # Kafka 集群备份 / 恢复
//!
//! 备份以**整个 Kafka 集群**为单位：集群本身（含认证信息）、其下全部 Kafka Connect
//! 与全部上下游连接器。文件由前端下载保存，服务端不存档。
//!
//! 连接器条目沿用 Connect 卡片导入导出的字段口径（方向 / 名称 / connectorName /
//! 配置 / 备注 / 启停 / 下游所属上游 / 敏感字段），导入侧的校验与提示因此可以复用，
//! 不会出现两套字段与两套提示。
//!
//! 恢复只写库内纳管记录，**不会**在远端注册或删除连接器；`sasl_password` 照常导出，
//! 页面必须明确提示文件含敏感信息。

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// 备份文件格式标识
pub const BACKUP_FORMAT: &str = "dbz-cluster-backup";
/// 备份文件版本
pub const BACKUP_VERSION: u32 = 1;

/// # 下游连接器所属上游
///
/// `id` 只用于追溯来源，恢复时按 `connectorName` 重新挂载。
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzBackupUpstreamVo {
    /// 来源环境里的上游记录 ID（仅追溯）
    #[serde(default)]
    pub id: Option<String>,
    /// 上游展示名称
    #[serde(default)]
    pub name: String,
    /// 上游连接器名称，恢复时按它挂载
    pub connector_name: String,
}

/// # 文件里的一个连接器
///
/// 与 Connect 导入导出的连接器条目同构，便于复用校验与冲突判定口径。
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzBackupConnectorVo {
    /// `source` / `sink`
    pub direction: String,
    /// 库内展示名称
    pub name: String,
    /// Kafka Connect 上的连接器名称
    pub connector_name: String,
    /// 连接器配置（JSON 文本，原样保存）
    pub connector_config: String,
    /// 备注
    #[serde(default)]
    pub remark: Option<String>,
    /// 启用
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// 下游才有：所属上游
    #[serde(default)]
    pub upstream: Option<DbzBackupUpstreamVo>,
    /// 本次导出命中的凭据字段（键名）
    #[serde(default)]
    pub sensitive_config_keys: Vec<String>,
}

/// # 文件里的一台 Kafka Connect
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzBackupConnectVo {
    /// 名称
    pub name: String,
    /// REST 地址
    pub url: String,
    /// 备注
    #[serde(default)]
    pub remark: Option<String>,
    /// 启用
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// 该 Connect 下的连接器：上游在前、下游在后
    #[serde(default)]
    pub connectors: Vec<DbzBackupConnectorVo>,
}

/// # 文件里的 Kafka 集群
///
/// 含 `sasl_password`：不导出密码，恢复后连不上集群，备份就失去意义。
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzBackupBrokerVo {
    /// 名称
    pub name: String,
    /// Broker 地址（`host:port`，多个用逗号分隔）
    pub bootstrap_servers: String,
    /// 安全协议，取值见 `KafkaSecurityProtocol`
    #[serde(default)]
    pub security_protocol: i16,
    /// SASL 机制，取值见 `KafkaSaslMechanism`
    #[serde(default)]
    pub sasl_mechanism: i16,
    /// 认证用户名
    #[serde(default)]
    pub sasl_username: Option<String>,
    /// 认证密码（敏感）
    #[serde(default)]
    pub sasl_password: Option<String>,
    /// 备注
    #[serde(default)]
    pub remark: Option<String>,
    /// 启用
    #[serde(default = "default_true")]
    pub enabled: bool,
}

/// # 导出的备份文件
///
/// 顶层结构：`format` / `version` / `exportedAt` / `credentialMode` / `broker` /
/// `connects`。`connects[].connectors[]` 复用 Connect 导入的连接器条目结构。
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzBackupFileVo {
    /// 固定为 [`BACKUP_FORMAT`]
    pub format: String,
    /// 固定为 [`BACKUP_VERSION`]
    pub version: u32,
    /// 导出时间（RFC3339）
    pub exported_at: String,
    /// 凭据口径：本接口固定 `included`
    pub credential_mode: String,
    /// Kafka 集群
    pub broker: DbzBackupBrokerVo,
    /// 该集群下的 Kafka Connect
    pub connects: Vec<DbzBackupConnectVo>,
}

/// # 恢复计划 / 结果里的一条记录
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzBackupRestoreItemVo {
    /// 层级：`broker` / `connect` / `source` / `sink`
    pub level: String,
    /// 所属上层名称：连接器为 Connect 名称，Connect 为集群名称
    #[serde(default)]
    pub parent: Option<String>,
    /// 记录展示名称
    pub name: String,
    /// 连接器名称（连接器条目才有）
    #[serde(default)]
    pub connector_name: Option<String>,
    /// `create` 新增 / `overwrite` 覆盖 / `skip` 跳过 / `failed` 失败
    pub action: String,
    /// 补充说明（跳过原因、失败原因）
    #[serde(default)]
    pub message: Option<String>,
}

/// # 恢复结果
///
/// 请求带 `dryRun=true` 时只返回计划，不写库，用于弹窗预览；`dryRun=false` 时按同一
/// 套判定真实写库，因此预览与执行不会出现两套口径。
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzBackupRestoreVo {
    /// 文件是否通过校验；false 时 `errors` 给出逐条原因
    pub valid: bool,
    /// 校验失败原因
    #[serde(default)]
    pub errors: Vec<String>,
    /// 本次策略：`skip` / `overwrite`
    pub strategy: String,
    /// 是否为预览
    pub dry_run: bool,
    /// 新增记录数
    pub created: u32,
    /// 覆盖记录数
    pub overwritten: u32,
    /// 跳过记录数
    pub skipped: u32,
    /// 失败记录数
    pub failed: u32,
    /// 明细
    #[serde(default)]
    pub items: Vec<DbzBackupRestoreItemVo>,
}

fn default_true() -> bool {
    true
}
