//! # Debezium 服务配置
//!
//! 定义 dbz 服务自身相关的配置项，包括连接器状态检查间隔与 jobx 执行器编码。
//! 该配置作为 [`crate::config::AppConfig`] 的一个配置段被读取。

use arc_swap::ArcSwapOption;
use jobx_wkr::{JobxExecutorConfig, JobxJobConfig, resolve_job_group_name};
use robotech::cfg::CfgError;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

pub const DBZ_CONFIG_KEY: &str = "dbz";
/// Debezium 计划分组名称的默认值
///
/// 连接器健康检查是 dbz 自己的周期计划，注册时统一挂到这个分组下。
/// 部署可用 `[dbz].group-name` 覆盖，留空时回落到这里。
pub const DBZ_JOB_GROUP_NAME: &str = "Debezium";
/// Debezium 计划分组备注
pub const DBZ_JOB_GROUP_REMARK: &str = "Debezium 连接器健康检查计划";

static DBZ_CONFIG: ArcSwapOption<DbzConfig> = ArcSwapOption::const_empty();

pub fn get_dbz_config() -> Result<Arc<DbzConfig>, CfgError> {
    DBZ_CONFIG
        .load_full()
        .ok_or(CfgError::NotInit("DBZ config not initialized".to_string()))
}

pub fn set_dbz_config(config: &DbzConfig) {
    DBZ_CONFIG.store(Some(Arc::new(config.clone())));
}

/// # Debezium 服务配置
///
/// 对应配置文件中的 `[dbz]` 配置段。
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct DbzConfig {
    /// jobx 计划分组名称；未配置或留空时使用 [`DBZ_JOB_GROUP_NAME`]
    #[serde(default = "dbz_job_group_name_default")]
    pub group_name: String,
    /// 健康检查配置
    #[serde(default)]
    pub health_check: HealthCheckConfig,
    /// 死信与重试队列配置
    #[serde(default)]
    pub dead_letter: DeadLetterConfig,
}

impl Default for DbzConfig {
    fn default() -> Self {
        Self {
            group_name: dbz_job_group_name_default(),
            health_check: HealthCheckConfig::default(),
            dead_letter: DeadLetterConfig::default(),
        }
    }
}

impl DbzConfig {
    /// 实际使用的 jobx 计划分组名称
    ///
    /// 配置有值时使用配置值；缺失或只有空白时回落到默认常量。注册计划与补齐
    /// 已存在计划都从这里取，避免两处按不同的名字查重。
    pub fn job_group_name(&self) -> String {
        resolve_job_group_name(&self.group_name, DBZ_JOB_GROUP_NAME)
    }
}

fn dbz_job_group_name_default() -> String {
    DBZ_JOB_GROUP_NAME.to_string()
}

/// # 死信与重试配置
///
/// 对应 `[dbz.dead-letter]`。默认值按「保留 30 天、单连接器最多 5 万条」设定；
/// 重试间隔 1s → 5s → 30s → 2m → 10m，超过 10 次转死信。
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct DeadLetterConfig {
    /// 是否启用后台重试 worker（关掉后只写死信，不自动重投）
    #[serde(default = "dead_letter_enabled_default")]
    pub enabled: bool,
    /// 死信保留天数
    #[serde(default = "dead_letter_retention_days_default")]
    pub retention_days: u32,
    /// 单个连接器的死信条数上限，超出按 `last_seen_ms` 淘汰最旧的
    #[serde(default = "dead_letter_max_per_connector_default")]
    pub max_per_connector: u32,
    /// 传输类失败的最大重试次数，超过转死信
    #[serde(default = "dead_letter_max_attempts_default")]
    pub max_attempts: u32,
    /// worker 轮询间隔（毫秒）
    #[serde(default = "dead_letter_worker_interval_ms_default")]
    pub worker_interval_ms: u64,
    /// 每轮最多处理的重试条数
    #[serde(default = "dead_letter_batch_size_default")]
    pub batch_size: u64,
    /// 单条重投的 HTTP 超时（毫秒）
    #[serde(default = "dead_letter_request_timeout_ms_default")]
    pub request_timeout_ms: u64,
}

impl Default for DeadLetterConfig {
    fn default() -> Self {
        Self {
            enabled: dead_letter_enabled_default(),
            retention_days: dead_letter_retention_days_default(),
            max_per_connector: dead_letter_max_per_connector_default(),
            max_attempts: dead_letter_max_attempts_default(),
            worker_interval_ms: dead_letter_worker_interval_ms_default(),
            batch_size: dead_letter_batch_size_default(),
            request_timeout_ms: dead_letter_request_timeout_ms_default(),
        }
    }
}

fn dead_letter_enabled_default() -> bool {
    true
}

fn dead_letter_retention_days_default() -> u32 {
    30
}

fn dead_letter_max_per_connector_default() -> u32 {
    50_000
}

fn dead_letter_max_attempts_default() -> u32 {
    10
}

fn dead_letter_worker_interval_ms_default() -> u64 {
    1_000
}

fn dead_letter_batch_size_default() -> u64 {
    50
}

fn dead_letter_request_timeout_ms_default() -> u64 {
    15_000
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
/// 默认健康检查计划的「任务保留时长」
///
/// 这个计划每 60s 跑一次，`jobx_task` 每天长 1440 行、一年 50 万行。健康检查的历史
/// 只有「最近有没有出过问题」这一个用途，7 天足够覆盖一个周末加两个工作日；更早的
/// 记录留着只会拖慢任务列表与概览统计。
///
/// 由 jobx-svr 的 `jobx-task-cleaner` 按 `jobx_task.create_ms` 判定并删除。
const HEALTH_CHECK_TASK_EXPIRE_DEFAULT: Duration = Duration::from_hours(24 * 7);
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
        .interval_duration(Some(HEALTH_CHECK_INTERVAL_DEFAULT))
        .stream_max_len(Some(HEALTH_CHECK_STREAM_MAX_LEN_DEFAULT))
        .task_expire_duration(Some(HEALTH_CHECK_TASK_EXPIRE_DEFAULT))
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

#[cfg(test)]
mod tests {
    use super::{DBZ_JOB_GROUP_NAME, DbzConfig};

    /// 配置有值时使用配置值
    #[test]
    fn configured_group_name_is_used() {
        let config: DbzConfig =
            serde_json::from_value(serde_json::json!({"group-name": "生产 Debezium"})).unwrap();
        assert_eq!(config.job_group_name(), "生产 Debezium");
    }

    /// 配置缺失时回落到默认常量
    #[test]
    fn missing_group_name_falls_back_to_default() {
        let config: DbzConfig = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(config.job_group_name(), DBZ_JOB_GROUP_NAME);
    }

    /// 配置为空或只有空白时回落到默认常量
    #[test]
    fn blank_group_name_falls_back_to_default() {
        let config: DbzConfig =
            serde_json::from_value(serde_json::json!({"group-name": "   "})).unwrap();
        assert_eq!(config.job_group_name(), DBZ_JOB_GROUP_NAME);
    }

    /// 配置改动后再次启动仍使用同一个配置值，按名称查重不会重复建组
    #[test]
    fn configured_group_name_is_stable_across_restarts() {
        let first: DbzConfig =
            serde_json::from_value(serde_json::json!({"group-name": "生产 Debezium"})).unwrap();
        let second: DbzConfig =
            serde_json::from_value(serde_json::json!({"group-name": "生产 Debezium"})).unwrap();
        assert_eq!(first.job_group_name(), second.job_group_name());
        assert_eq!(first.job_group_name(), "生产 Debezium");
        assert_ne!(first.job_group_name(), DBZ_JOB_GROUP_NAME);
    }
}
