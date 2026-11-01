use alarm_api_client::setup_alarm_api_client;
use anyhow::anyhow;
use config::Value;
use dbz_svr::config::{AppConfig, setup_app_config};
use dbz_svr::utils::{setup_dbz_alarm, setup_dbz_config};
use jobx_api_client::setup_jobx_api_client;
use robotech::app::setup_id_worker;
use robotech::db::setup_db_conn;
use robotech::macros::{bootstrap, db_migrate, log_call};
use robotech::redis::setup_redis_conn;
use robotech::web::setup_web_server;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::warn;

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

    setup_app_config(app_config.as_ref().clone(), changed);
    setup_id_worker(app_config.id_worker.clone(), changed).await?;
    setup_redis_conn(app_config.redis.clone(), changed).await?;
    setup_db_conn(app_config.db.clone(), changed).await?;
    setup_jobx_api_client(app_config.api.clone(), changed).await?;

    // 报警中心是选配：没写 `[api.alarm]` 就跳过告警，巡检照常跑（升级前的老
    // 部署就是这种形态，不该因为多了一个告警功能起不来）。
    //
    // 配了就必须登记成功：连接器异常事件的策略、来源、级别由 dbz 自己在首次
    // 启动时登记到报警中心，报警中心只认识登记过的策略。登记要在**启动巡检
    // 之前**完成——下面的 setup_dbz_config 会初始化健康检查执行器，它随时
    // 可能跑起来发第一条告警，顺序反过来第一次启动的告警会被以「查不到策略」
    // 拒收（只有一条 warn，很容易被当成没配置）。
    //
    // 只在 `changed.is_none()`（首次加载配置）时登记：登记本身幂等，重启天然
    // 会重跑一遍；热更新时不必再走。反过来说，运行期才补上 `[api.alarm]` 的
    // 部署要重启一次 dbz，事件才会登记上去。
    if app_config.api.contains_key("alarm") {
        setup_alarm_api_client(app_config.api.clone(), changed).await?;
        if changed.is_none() {
            setup_dbz_alarm(&app_config.alarm).await?;
        }
    } else {
        warn!("未配置 [api.alarm]，连接器异常告警不可用（巡检与连接器管理不受影响）");
    }

    setup_web_server(app_config.web.clone(), port, old_pid, changed).await?;

    setup_dbz_config(app_config.dbz.clone(), changed).await?;

    Ok(())
}
