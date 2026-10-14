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
    #[serde(default = "health_check_executor_code_default")]
    pub executor_code: String,
    /// 任务计划配置
    #[serde(default = "health_check_job_default")]
    pub job: JobxJobConfig,
    /// 任务执行器配置
    #[serde(default = "health_check_executor_default")]
    pub executor: JobxExecutorConfig,
}

/// 默认健康检查的任务执行器编码
const HEALTH_CHECK_EXECUTOR_CODE_DEFAULT: &str = "dbz.health-check";
/// 默认健康检查的计划名称
const HEALTH_CHECK_JOB_NAME_DEFAULT: &str = "Debezium健康检查";
/// 默认健康检查的间隔时间
const HEALTH_CHECK_INTERVAL_DEFAULT: Duration = Duration::from_secs(60);
/// 默认健康检查计划的 Stream 长度上限
///
/// 比 `JobxJobConfig` 的全局默认值（10000）小两个数量级，理由是健康检查这个计划的
/// 特征很极端：每 60s 必发一条、且业务上只需要"当前立刻执行一次"，历史上堆积的
/// 重复消息没有任何价值。把它压到 100 能让故障重启后的积压最小化。
///
/// 100 同时也是 `MAXLEN ~` 的实际地板（按 macro node 整块裁剪，node 容量 100），
/// 所以这已经是最小值，再小没有意义。
const HEALTH_CHECK_STREAM_MAX_LEN_DEFAULT: u32 = 100;
/// 默认健康检查的任务执行器一次接收最大消息数
const HEALTH_CHECK_MAX_MESSAGES_DEFAULT: usize = 100000;
/// 默认健康检查的任务执行器是否只处理最新消息
const HEALTH_CHECK_LAST_ONLY_DEFAULT: bool = true;

impl Default for HealthCheckConfig {
    fn default() -> HealthCheckConfig {
        HealthCheckConfig {
            executor_code: health_check_executor_code_default(),
            job: health_check_job_default(),
            executor: health_check_executor_default(),
        }
    }
}

fn health_check_job_default() -> JobxJobConfig {
    JobxJobConfig::builder()
        .name(HEALTH_CHECK_JOB_NAME_DEFAULT.to_string())
        .executor_code(health_check_executor_code_default())
        .interval_duration(Some(HEALTH_CHECK_INTERVAL_DEFAULT.clone()))
        .stream_max_len(Some(HEALTH_CHECK_STREAM_MAX_LEN_DEFAULT))
        .build()
}

fn health_check_executor_default() -> JobxExecutorConfig {
    JobxExecutorConfig::builder()
        .executor_code(health_check_executor_code_default())
        .max_messages(HEALTH_CHECK_MAX_MESSAGES_DEFAULT)
        .last_only(HEALTH_CHECK_LAST_ONLY_DEFAULT)
        .build()
}

fn health_check_executor_code_default() -> String {
    HEALTH_CHECK_EXECUTOR_CODE_DEFAULT.to_string()
}
