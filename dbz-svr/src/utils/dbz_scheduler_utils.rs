use crate::config::AppConfig;
use config::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

pub fn setup_dbz_scheduler(
    _app_config: Arc<AppConfig>,
    _changed: &Option<HashMap<String, Value>>,
) {
    info!("setup dbz scheduler config...");
}