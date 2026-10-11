//! # Debezium 实例配置
//!
//! 定义 dbz 服务自身相关的配置项，包括连接器状态检查间隔与 jobx 执行器编码。
//! 该配置作为 [`crate::config::AppConfig`] 的一个配置段被读取。

use serde::{Deserialize, Serialize};
use std::time::Duration;
use wheel_rs::serde::duration_serde;

/// 连接器状态检查的默认间隔
const DEFAULT_CHECK_INTERVAL_SECS: u64 = 30;

/// jobx 定时检查任务的默认执行器编码
const DEFAULT_EXECUTOR_CODE: &str = "dbz-svr";

/// # Debezium 实例配置
///
/// 对应配置文件中的 `[dbz]` 配置段。
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct DbzConfig {
    /// 连接器状态检查间隔
    ///
    /// 同时用于：内置调度器扫描周期，以及注册实例时创建 jobx 定时检查任务的间隔。
    #[serde(default = "default_check_interval", with = "duration_serde")]
    pub check_interval: Duration,
    /// jobx 定时检查任务的执行器编码
    ///
    /// 只有订阅了该编码的 dbz-svr 实例才会收到 jobx 分派的检查任务。
    #[serde(default = "default_executor_code")]
    pub executor_code: String,
}

impl Default for DbzConfig {
    fn default() -> Self {
        DbzConfig {
            check_interval: default_check_interval(),
            executor_code: default_executor_code(),
        }
    }
}

fn default_check_interval() -> Duration {
    Duration::from_secs(DEFAULT_CHECK_INTERVAL_SECS)
}

fn default_executor_code() -> String {
    DEFAULT_EXECUTOR_CODE.to_string()
}
