use crate::config::AppConfig;
use crate::svc::{set_app_config, DbzTaskSvc};
use config::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};
use wheel_rs::time_utils::build_ticker;

pub fn setup_dbz_scheduler(
    app_config: Arc<AppConfig>,
    changed: &Option<HashMap<String, Value>>,
) {
    info!("setup dbz scheduler config...");
    set_app_config((*app_config).clone());

    // 如果是首次配置，启动扫描线程
    if changed.is_none() {
        tokio::spawn(run_scheduler_loop());
    }
}

/// # 启动调度器扫描循环
///
/// 按固定间隔（30秒）定时扫描所有已启用的任务并检查其连接器状态。
/// 该函数不会返回，需通过 tokio::spawn 在独立任务中运行。
async fn run_scheduler_loop() {
    let scan_interval = Duration::from_secs(30);
    let mut ticker = build_ticker(scan_interval);
    loop {
        ticker.tick().await;
        if let Err(e) = DbzTaskSvc::scan_and_check_status().await {
            warn!("DBZ 调度器扫描异常: {e:?}");
        }
    }
}