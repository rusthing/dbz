use crate::config::DbzConfig;
use arc_swap::ArcSwapOption;
use config::Value;
use idworker::IdWorkerConfig;
use robotech::cfg::CfgError;
use robotech::db::DbConnConfig;
use robotech::redis::RedisConfig;
use robotech::web::WebServerConfig;
use serde::Deserialize;
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
    /// jobx
    pub jobx: JobxConfig,
    /// alarm
    pub alarm: AlarmConfig,
}

/// # jobx 配置
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct JobxConfig {
    /// jobx-svr 服务地址（用于创建定时检查任务计划）
    pub base_url: String,
}

/// # alarm 配置
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct AlarmConfig {
    /// alarm-svr 服务地址（用于发送告警）
    pub base_url: String,
    /// 告警事件编码（需在 alarm-svr 中预先配置对应的事件来源与策略）
    #[serde(default)]
    pub event_code: Option<String>,
    /// 告警ID（对应 alarm-svr 中的 alarm_alert.id，未配置时跳过告警上报）
    #[serde(default)]
    pub alert_id: Option<u64>,
}
