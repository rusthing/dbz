use crate::config::{DBZ_CONFIG_KEY, DbzConfig, set_dbz_config};
use crate::svc::DbzKafkaConnectSvc;
use anyhow::bail;
use async_trait::async_trait;
use config::Value;
use jobx_api::vo::JobxTaskVo;
use jobx_api_client::get_jobx_api_client;
use jobx_wkr::{JobxTaskHandler, init_jobx_executor, set_jobx_executor_config};
use robotech::api::RO_CODE_WARNING_DUPLICATE_KEY;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;
use wheel_rs::config_utils::has_config_changed;

struct HealthCheckHandler;

#[async_trait]
impl JobxTaskHandler for HealthCheckHandler {
    async fn handle(&self, _task: &JobxTaskVo) -> anyhow::Result<String> {
        DbzKafkaConnectSvc::check_all_connectors().await
    }
}

pub async fn setup_dbz_config(
    dbz_config: DbzConfig,
    changed: &Option<HashMap<String, Value>>,
) -> anyhow::Result<()> {
    info!("setup dbz config...: {dbz_config:?}");
    if changed
        .as_ref()
        .map(|changed| has_config_changed(DBZ_CONFIG_KEY, changed))
        .unwrap_or(true)
    {
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
            // 添加健康检查任务计划
            let jobx_api_client = get_jobx_api_client()?;
            let job_add_dto: jobx_api::dto::JobxJobAddDto = health_check_job_config.into();
            match jobx_api_client.job.add(&job_add_dto).await {
                Ok(ro) => {
                    info!("health check job added result: {ro:?}");
                    if ro.is_ok() || ro.is_warn_code(RO_CODE_WARNING_DUPLICATE_KEY) {
                        info!("health check job added successfully");
                    } else {
                        bail!("health check job added failed: {ro:?}");
                    }
                }
                Err(e) => {
                    bail!("health check job added failed: {e:?}");
                }
            }
        } else {
            set_jobx_executor_config(dbz_config.health_check.executor.clone());
        }
        set_dbz_config(&dbz_config);
    }
    Ok(())
}
