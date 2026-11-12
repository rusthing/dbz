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
    /// Kafka Connect 死信队列配置
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

/// # Kafka Connect 死信队列配置
///
/// 对应 `[dbz.dead-letter]`。坏记录的**产生、重试、落地**全部交给 Kafka Connect：
/// 这里的值在部署下游连接器时被写进 `connector_config`（见
/// [`crate::utils::ensure_sink_dlq_config`]），dbz 只负责从 DLQ topic 同步记录供页面查询，
/// 以及人工处置。dbz 侧不再有重试队列、退避 worker 和「脚本显式上报」。
///
/// 注意 Connect 的 DLQ 只适用于 **sink** 连接器（source 侧无法把外部记录转成
/// Kafka 记录），所以这些 key 只注入下游连接器。
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct DeadLetterConfig {
    /// 是否启用 DLQ 配置注入与记录同步
    #[serde(default = "dead_letter_enabled_default")]
    pub enabled: bool,
    /// 全局 DLQ topic 名；所有下游连接器共用，靠 context header 区分来源
    #[serde(default = "dead_letter_topic_default")]
    pub topic: String,
    /// Connect 自动创建 DLQ topic 时的副本数
    #[serde(default = "dead_letter_replication_factor_default")]
    pub replication_factor: u16,
    /// `errors.retry.timeout`：框架层单次错误最长重试多久（毫秒）
    #[serde(default = "dead_letter_retry_timeout_ms_default")]
    pub retry_timeout_ms: u64,
    /// `errors.retry.delay.max.ms`：框架层重试的最大退避间隔（毫秒）
    #[serde(default = "dead_letter_retry_delay_max_ms_default")]
    pub retry_delay_max_ms: u64,
    /// 请求级重试次数：rebue 实现写 `http.retry.max`，Confluent 实现写 `max.retries`
    #[serde(default = "dead_letter_http_max_retries_default")]
    pub http_max_retries: u32,
    /// 请求级首次退避：rebue 实现写 `http.retry.backoff.ms`，Confluent 写 `retry.backoff.ms`
    #[serde(default = "dead_letter_http_retry_backoff_ms_default")]
    pub http_retry_backoff_ms: u64,
    /// `errors.log.enable`：把错误上下文写进 Connect 日志
    #[serde(default = "dead_letter_log_enable_default")]
    pub log_enable: bool,
    /// `errors.log.include.messages`：日志里带上原始报文（可能含敏感信息）
    #[serde(default = "dead_letter_log_include_messages_default")]
    pub log_include_messages: bool,
    /// 每轮每个 DLQ 分区最多同步多少条记录
    #[serde(default = "dead_letter_sync_batch_size_default")]
    pub sync_batch_size: u32,
    /// 页面索引的保留天数（DLQ topic 本身的保留由 Kafka 侧配置，互不影响）
    #[serde(default = "dead_letter_retention_days_default")]
    pub retention_days: u32,
    /// 单个连接器在页面索引里最多保留多少条
    #[serde(default = "dead_letter_max_per_connector_default")]
    pub max_per_connector: u32,
    /// DLQ 增量同步计划：独立 jobx 计划，与健康检查解耦，可单独调频率与执行器
    #[serde(default)]
    pub sync_job: DeadLetterSyncJobConfig,
}

impl Default for DeadLetterConfig {
    fn default() -> Self {
        Self {
            enabled: dead_letter_enabled_default(),
            topic: dead_letter_topic_default(),
            replication_factor: dead_letter_replication_factor_default(),
            retry_timeout_ms: dead_letter_retry_timeout_ms_default(),
            retry_delay_max_ms: dead_letter_retry_delay_max_ms_default(),
            http_max_retries: dead_letter_http_max_retries_default(),
            http_retry_backoff_ms: dead_letter_http_retry_backoff_ms_default(),
            log_enable: dead_letter_log_enable_default(),
            log_include_messages: dead_letter_log_include_messages_default(),
            sync_batch_size: dead_letter_sync_batch_size_default(),
            retention_days: dead_letter_retention_days_default(),
            max_per_connector: dead_letter_max_per_connector_default(),
            sync_job: DeadLetterSyncJobConfig::default(),
        }
    }
}

/// # DLQ 增量同步计划配置
///
/// 对应 `[dbz.dead-letter.sync-job]`。DLQ 同步是写库类周期任务，按 dbz 周期任务约定
/// 必须走独立 jobx 计划（不再像早期那样挂在健康检查计划里顺带跑）：它的频率可与健康
/// 检查分开调，健康检查是否成功也不影响死信同步，二者语义各自干净。
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct DeadLetterSyncJobConfig {
    /// 任务执行器的编码
    #[serde(default = "dead_letter_sync_executor_code_default")]
    pub executor_code: String,
    /// 任务计划配置
    #[serde(default = "dead_letter_sync_job_default")]
    pub job: JobxJobConfig,
    /// 任务执行器配置
    #[serde(default = "dead_letter_sync_executor_default")]
    pub executor: JobxExecutorConfig,
}

/// 默认 DLQ 同步计划的执行器编码
const DEAD_LETTER_SYNC_EXECUTOR_CODE_DEFAULT: &str = "dbz.dead-letter-sync";
/// 默认 DLQ 同步计划的名称
const DEAD_LETTER_SYNC_JOB_NAME_DEFAULT: &str = "Debezium死信同步";
/// 默认 DLQ 同步计划的间隔时间
///
/// 与健康检查同频（60s）以保持既有行为；现在两者解耦，需要更慢只需改这一个配置。
const DEAD_LETTER_SYNC_INTERVAL_DEFAULT: Duration = Duration::from_secs(60);
/// 默认 DLQ 同步计划的「任务保留时长」
///
/// 与健康检查一致：同步历史只用来排查「最近有没有同步过 / 出没出过错」，7 天足够。
const DEAD_LETTER_SYNC_TASK_EXPIRE_DEFAULT: Duration = Duration::from_hours(24 * 7);
/// 默认 DLQ 同步计划的 Stream 长度上限
///
/// 同步计划同样只关心「当前立刻跑一次」，历史堆积无意义，压到 100 让故障重启后的
/// 积压最小化（与健康检查一致，100 也是 MAXLEN 的实际地板）。
const DEAD_LETTER_SYNC_STREAM_MAX_LEN_DEFAULT: u32 = 100;
/// 默认 DLQ 同步计划的执行器一次接收最大消息数
const DEAD_LETTER_SYNC_MAX_MESSAGES_DEFAULT: usize = 100000;
/// 默认 DLQ 同步计划的执行器是否只处理最新消息
const DEAD_LETTER_SYNC_LAST_ONLY_DEFAULT: bool = true;

impl Default for DeadLetterSyncJobConfig {
    fn default() -> Self {
        Self {
            executor_code: dead_letter_sync_executor_code_default(),
            job: dead_letter_sync_job_default(),
            executor: dead_letter_sync_executor_default(),
        }
    }
}

fn dead_letter_sync_job_default() -> JobxJobConfig {
    JobxJobConfig::builder()
        .name(DEAD_LETTER_SYNC_JOB_NAME_DEFAULT.to_string())
        .executor_code(dead_letter_sync_executor_code_default())
        .interval_duration(Some(DEAD_LETTER_SYNC_INTERVAL_DEFAULT))
        .stream_max_len(Some(DEAD_LETTER_SYNC_STREAM_MAX_LEN_DEFAULT))
        .task_expire_duration(Some(DEAD_LETTER_SYNC_TASK_EXPIRE_DEFAULT))
        .build()
}

fn dead_letter_sync_executor_default() -> JobxExecutorConfig {
    JobxExecutorConfig::builder()
        .executor_code(dead_letter_sync_executor_code_default())
        .max_messages(DEAD_LETTER_SYNC_MAX_MESSAGES_DEFAULT)
        .last_only(DEAD_LETTER_SYNC_LAST_ONLY_DEFAULT)
        .build()
}

fn dead_letter_sync_executor_code_default() -> String {
    DEAD_LETTER_SYNC_EXECUTOR_CODE_DEFAULT.to_string()
}

fn dead_letter_enabled_default() -> bool {
    true
}

fn dead_letter_topic_default() -> String {
    "dbz-connect-dlq".to_string()
}

fn dead_letter_replication_factor_default() -> u16 {
    3
}

fn dead_letter_retry_timeout_ms_default() -> u64 {
    300_000
}

fn dead_letter_retry_delay_max_ms_default() -> u64 {
    30_000
}

fn dead_letter_http_max_retries_default() -> u32 {
    10
}

fn dead_letter_http_retry_backoff_ms_default() -> u64 {
    3_000
}

fn dead_letter_log_enable_default() -> bool {
    true
}

fn dead_letter_log_include_messages_default() -> bool {
    false
}

fn dead_letter_sync_batch_size_default() -> u32 {
    200
}

fn dead_letter_retention_days_default() -> u32 {
    30
}

fn dead_letter_max_per_connector_default() -> u32 {
    50_000
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
