use anyhow::anyhow;
use config::Value;
use dbz_svr::config::AppConfig;
use dbz_svr::utils::setup_dbz_scheduler;
use idworker::setup_id_worker;
use robotech::db::setup_db_conn;
use robotech::macros::{bootstrap, db_migrate, log_call};
use robotech::redis::setup_redis_conn;
use robotech::web::setup_web_server;
use std::collections::HashMap;
use std::sync::Arc;

bootstrap!(AppConfig);

#[log_call]
async fn setup(
    app_config: &Arc<AppConfig>,
    changed: &Option<HashMap<String, Value>>,
    port: Option<u16>,
    old_pid: Option<u32>,
) -> Result<(), anyhow::Error> {
    let db_url = app_config.db.get_url();
    db_migrate!(db_url);

    setup_id_worker(app_config.id_worker.clone(), changed)?;
    setup_redis_conn(app_config.redis.clone(), changed).await?;
    setup_db_conn(app_config.db.clone(), changed).await?;

    setup_dbz_scheduler(app_config.clone(), changed);
    setup_web_server(app_config.web.clone(), port, old_pid, changed).await?;

    Ok(())
}