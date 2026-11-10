use crate::config::DBZ_JOB_GROUP_REMARK;
use crate::config::{DBZ_CONFIG_KEY, DbzConfig, set_dbz_config};
use crate::svc::{DbzDeadLetterSvc, DbzKafkaConnectSvc};
use anyhow::bail;
use async_trait::async_trait;
use config::Value;
use jobx_api::vo::JobxTaskVo;
use jobx_api_client::get_jobx_api_client;
use jobx_wkr::{
    JobxTaskHandler, ensure_existing_job_group, ensure_job_group, init_jobx_executor,
    set_jobx_executor_config,
};
use robotech::api::{RO_CODE_WARNING_DUPLICATE_KEY, U64};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};
use wheel_rs::config_utils::has_config_changed;

struct HealthCheckHandler;

#[async_trait]
impl JobxTaskHandler for HealthCheckHandler {
    async fn handle(&self, _task: &JobxTaskVo) -> anyhow::Result<String> {
        let health = DbzKafkaConnectSvc::check_all_connectors().await;
        // 死信同步与巡检共用同一个 jobx 计划：都是「连远端 → 写本地库」的周期动作，
        // 没必要再注册第二个执行器。同步失败不能让健康检查变成失败，只记日志；
        // 按 Kafka 坐标幂等落库，多副本各跑一次也不会重复或覆盖人工结论。
        if let Err(e) = DbzDeadLetterSvc::sync_from_dlq().await {
            warn!("同步 Kafka Connect DLQ 失败: {e}");
        }
        health
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
            // 添加健康检查任务计划（挂到配置的分组下）
            let group_name = dbz_config.job_group_name();
            let group_id = ensure_job_group(&group_name, Some(DBZ_JOB_GROUP_REMARK)).await?;
            let jobx_api_client = get_jobx_api_client()?;
            let mut job_add_dto: jobx_api::dto::JobxJobAddDto = health_check_job_config.into();
            job_add_dto.group_id = Some(Some(U64::from(group_id)));
            match jobx_api_client.job.add(&job_add_dto).await {
                Ok(ro) => {
                    info!("health check job added result: {ro:?}");
                    if ro.is_ok() {
                        info!("health check job added successfully");
                    } else if ro.is_warn_code(RO_CODE_WARNING_DUPLICATE_KEY) {
                        ensure_existing_job_group(&dbz_config.health_check.job.name, group_id)
                            .await?;
                        info!("health check job already exists, group confirmed");
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
