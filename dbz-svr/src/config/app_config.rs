use crate::config::DbzConfig;
use arc_swap::ArcSwapOption;
use config::Value;
use idworker::IdWorkerConfig;
use robotech::api_client::ApiClientConfig;
use robotech::cfg::CfgError;
use robotech::db::DbConnConfig;
use robotech::redis::RedisConfig;
use robotech::web::WebServerConfig;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

/// # 全局应用配置
///
/// 使用 `ArcSwapOption` 保存，支持配置热更新时的无锁替换。
static APP_CONFIG: ArcSwapOption<AppConfig> = ArcSwapOption::const_empty();

/// # 获取全局应用配置
///
/// ## 返回值
/// * `Ok(Arc<AppConfig>)` - 应用配置的只读引用
/// * `Err(CfgError)` - 应用配置尚未初始化
pub fn get_app_config() -> Result<Arc<AppConfig>, CfgError> {
    APP_CONFIG
        .load_full()
        .ok_or(CfgError::NotInit("AppConfig not initialized".to_string()))
}

/// # 初始化全局应用配置
///
/// ## 参数
/// * `app_config` - 应用配置
/// * `changed` - 配置变更信息（应用配置为根配置，变更时整体替换，此处仅保留签名一致）
pub fn setup_app_config(app_config: AppConfig, _changed: &Option<HashMap<String, Value>>) {
    info!("setup app config...");
    APP_CONFIG.store(Some(Arc::new(app_config)));
}

/// # 配置文件结构
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct AppConfig {
    /// db
    pub db: DbConnConfig,
    /// Web服务器
    #[serde(default = "WebServerConfig::default")]
    pub web: WebServerConfig,
    /// id_worker
    #[serde(default = "IdWorkerConfig::default")]
    pub id_worker: IdWorkerConfig,
    /// redis
    pub redis: RedisConfig,
    /// dbz
    #[serde(default = "DbzConfig::default")]
    pub dbz: DbzConfig,
    /// API客户端设置
    #[serde(default = "HashMap::default")]
    pub api: HashMap<String, ApiClientConfig>,
    /// 告警配置
    #[serde(default = "AlarmConfig::default")]
    pub alarm: AlarmConfig,
}

/// # 告警配置
///
/// 对应配置文件中的 `[alarm]` 配置段，用于向 alarm-svr 上报实例连接器异常事件。
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct AlarmConfig {
    /// alarm-svr 的 API 地址
    #[serde(default)]
    pub base_url: String,
    /// 告警事件编码（需在 alarm-svr 中预先配置对应的事件来源与策略）
    #[serde(default)]
    pub event_code: Option<String>,
    /// 告警ID（对应 alarm-svr 中的 alarm_alert.id，不填则跳过告警上报）
    #[serde(default)]
    pub alert_id: Option<u64>,
}
