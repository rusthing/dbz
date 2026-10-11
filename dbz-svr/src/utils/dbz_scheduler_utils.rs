//! # Debezium 调度工具
//!
//! 启动内置的实例扫描调度器：按 `[dbz] check-interval` 周期扫描所有已启用的实例，
//! 检查其上游/下游连接器状态，并在异常时触发告警与自动重启。

use crate::config::{AppConfig, get_app_config};
use crate::svc::DbzInstanceSvc;
use config::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};
use wheel_rs::time_utils::build_ticker;

/// # 初始化 Debezium 调度器
///
/// 仅在首次启动（`changed` 为 `None`）时开启扫描循环，配置热更新时不会重复启动。
///
/// ## 参数
/// * `_app_config` - 应用配置（全局配置已由 `setup_app_config` 保存）
/// * `changed` - 配置变更信息
pub fn setup_dbz_scheduler(
    _app_config: Arc<AppConfig>,
    changed: &Option<HashMap<String, Value>>,
) {
    info!("setup dbz scheduler config...");
    // 如果是首次配置，启动扫描循环
    if changed.is_none() {
        tokio::spawn(run_scheduler_loop());
    }
}

/// # 调度器扫描循环
///
/// 按配置的检查间隔周期性扫描所有已启用的实例并检查其连接器状态。
/// 该函数不会返回，需通过 `tokio::spawn` 在独立任务中运行。
async fn run_scheduler_loop() {
    // 从全局配置读取检查间隔，配置尚未就绪时回退到默认值
    let scan_interval = get_app_config()
        .map(|config| config.dbz.check_interval)
        .unwrap_or_else(|_| Duration::from_secs(30));
    let mut ticker = build_ticker(scan_interval);
    loop {
        ticker.tick().await;
        if let Err(e) = DbzInstanceSvc::scan_and_check_status().await {
            warn!("DBZ 调度器扫描异常: {e:?}");
        }
    }
}
