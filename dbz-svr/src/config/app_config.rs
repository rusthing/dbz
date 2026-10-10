use idworker::IdWorkerConfig;
use robotech::db::DbConnConfig;
use robotech::redis::RedisConfig;
use robotech::web::WebServerConfig;
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct AppConfig {
    pub db: DbConnConfig,
    #[serde(default = "WebServerConfig::default")]
    pub web: WebServerConfig,
    #[serde(default = "IdWorkerConfig::default")]
    pub id_worker: IdWorkerConfig,
    pub redis: RedisConfig,
    pub jobx: JobxConfig,
    pub alarm: AlarmConfig,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct JobxConfig {
    /// jobx-svr 服务地址（用于创建定时检查任务）
    pub base_url: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct AlarmConfig {
    /// alarm-svr 服务地址（用于发送告警）
    pub base_url: String,
}