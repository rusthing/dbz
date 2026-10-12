//! # Debezium 实例配置
//!
//! 定义 dbz 服务自身相关的配置项，包括连接器状态检查间隔与 jobx 执行器编码。
//! 该配置作为 [`crate::config::AppConfig`] 的一个配置段被读取。

use arc_swap::ArcSwapOption;
use jobx_wkr::{JobxExecutorConfig, JobxJobConfig};
use robotech::cfg::CfgError;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

pub const DBZ_CONFIG_KEY: &str = "dbz";
static DBZ_CONFIG: ArcSwapOption<DbzConfig> = ArcSwapOption::const_empty();

pub fn get_dbz_config() -> Result<Arc<DbzConfig>, CfgError> {
    DBZ_CONFIG
        .load_full()
        .ok_or(CfgError::NotInit("DBZ config not initialized".to_string()))
}

pub fn set_dbz_config(config: &DbzConfig) {
    DBZ_CONFIG.store(Some(Arc::new(config.clone())));
}

/// # Debezium 实例配置
///
/// 对应配置文件中的 `[dbz]` 配置段。
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct DbzConfig {
    /// 健康检查配置
    #[serde(default)]
    pub health_check: HealthCheckConfig,
}

/// # 检查配置
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct HealthCheckConfig {
    /// 任务执行器的编码
    #[serde(default = "executor_code_default")]
    pub executor_code: String,
    /// 任务计划配置
    #[serde(default = "job_default")]
    pub job: JobxJobConfig,
    /// 任务执行器配置
    #[serde(default = "executor_default")]
    pub executor: JobxExecutorConfig,
}

/// 默认健康检查的任务执行器编码
const HEALTH_CHECK_EXECUTOR_CODE_DEFAULT: &str = "dbz.health-check";
/// 默认健康检查的计划名称
const HEALTH_CHECK_JOB_NAME_DEFAULT: &str = "Debezium健康检查";
/// 默认健康检查的间隔时间
const HEALTH_CHECK_INTERVAL_DEFAULT: Duration = Duration::from_secs(60);

impl Default for HealthCheckConfig {
    fn default() -> HealthCheckConfig {
        HealthCheckConfig {
            executor_code: executor_code_default(),
            job: job_default(),
            executor: executor_default(),
        }
    }
}

fn job_default() -> JobxJobConfig {
    JobxJobConfig::builder()
        .name(HEALTH_CHECK_JOB_NAME_DEFAULT.to_string())
        .executor_code(executor_code_default())
        .interval_duration(Some(HEALTH_CHECK_INTERVAL_DEFAULT.clone()))
        .build()
}

fn executor_default() -> JobxExecutorConfig {
    JobxExecutorConfig::builder()
        .executor_code(executor_code_default())
        .build()
}

fn executor_code_default() -> String {
    HEALTH_CHECK_EXECUTOR_CODE_DEFAULT.to_string()
}
