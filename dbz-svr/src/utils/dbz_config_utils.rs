use crate::config::{AppConfig, DBZ_CONFIG_KEY, set_dbz_config};
use crate::svc::DbzInstanceSvc;
use async_trait::async_trait;
use config::Value;
use jobx_api::vo::JobxTaskVo;
use jobx_api_client::api_client::{get_jobx_api_client, setup_jobx_api_client};
use jobx_wkr::{JobxTaskHandler, init_jobx_executor};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};
use wheel_rs::config_utils::has_config_changed;

struct HealthCheckHandler;

#[async_trait]
impl JobxTaskHandler for HealthCheckHandler {
    async fn handle(&self, _task: &JobxTaskVo) {
        info!("健康检查任务开始执行...");
        match DbzInstanceSvc::check_all_connectors().await {
            Ok(errors) => {
                if errors.is_empty() {
                    info!("健康检查任务执行完成，一切正常");
                } else {
                    let detail = errors.join("; ");
                    warn!("健康检查任务执行完成，存在异常: {detail}");
                    panic!("健康检查异常: {detail}");
                }
            }
            Err(e) => {
                warn!("健康检查任务执行失败: {e}");
                panic!("健康检查任务执行失败: {e}");
            }
        }
    }
}

pub async fn setup_dbz_config(
    app_config: AppConfig,
    changed: &Option<HashMap<String, Value>>,
) -> anyhow::Result<()> {
    info!("setup dbz config...: {app_config:?}");
    if changed
        .as_ref()
        .map(|changed| has_config_changed(DBZ_CONFIG_KEY, changed))
        .unwrap_or(true)
    {
        setup_jobx_api_client(app_config.api, changed).await?;
        let dbz_config = app_config.dbz.clone();
        let health_check_job_config = dbz_config.health_check.job.clone();
        let health_check_executor_config = dbz_config.health_check.executor.clone();
        // 如果是首次配置
        if changed.is_none() {
            // 初始化执行器
            init_jobx_executor(
                health_check_executor_config.clone(),
                Arc::new(HealthCheckHandler),
            )
            .await?;
            // 添加计划
            let jobx_client = get_jobx_api_client()?;
            let add_dto: jobx_api::dto::JobxJobAddDto = health_check_job_config.into();
            jobx_client.job_client.add(&add_dto).await?;
        }
        set_dbz_config(&app_config.dbz.clone());
    }
    Ok(())
}