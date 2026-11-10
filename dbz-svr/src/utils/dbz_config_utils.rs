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
use tracing::info;
use wheel_rs::config_utils::has_config_changed;

struct HealthCheckHandler;

#[async_trait]
impl JobxTaskHandler for HealthCheckHandler {
    async fn handle(&self, _task: &JobxTaskVo) -> anyhow::Result<String> {
        DbzKafkaConnectSvc::check_all_connectors().await
    }
}

/// # DLQ 增量同步计划处理器
///
/// 独立的 jobx 计划（不再挂在健康检查里）。每次执行把 Kafka Connect 的 DLQ topic
/// 增量同步进 `dbz_dead_letter` 页面索引。同步失败（DB 故障、配置未启用等）直接返回
/// 错误，由 jobx 标记任务失败并走任务过期清理——不再像早期那样「只记日志、假装成功」。
/// 单个集群连不上已在 [`DbzDeadLetterSvc::sync_from_dlq`] 内部按集群隔离并记日志，
/// 不会让整轮失败。
struct DeadLetterSyncHandler;

#[async_trait]
impl JobxTaskHandler for DeadLetterSyncHandler {
    async fn handle(&self, _task: &JobxTaskVo) -> anyhow::Result<String> {
        let count = DbzDeadLetterSvc::sync_from_dlq().await?;
        Ok(format!("已同步 {count} 条死信"))
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
        // 首次配置：注册健康检查与 DLQ 同步两个独立执行器 + 计划；之后只更新执行器配置
        if changed.is_none() {
            // 执行器：健康检查与 DLQ 同步各一个（处理器不同，不能共用执行器）
            init_jobx_executor(
                dbz_config.health_check.executor.clone(),
                Arc::new(HealthCheckHandler),
            )
            .await?;
            init_jobx_executor(
                dbz_config.dead_letter.sync_job.executor.clone(),
                Arc::new(DeadLetterSyncHandler),
            )
            .await?;

            // 两个计划挂到同一个 jobx 分组下
            let group_name = dbz_config.job_group_name();
            let group_id = ensure_job_group(&group_name, Some(DBZ_JOB_GROUP_REMARK)).await?;
            let jobx_api_client = get_jobx_api_client()?;

            // 健康检查计划
            let mut health_job_add_dto: jobx_api::dto::JobxJobAddDto =
                dbz_config.health_check.job.clone().into();
            health_job_add_dto.group_id = Some(Some(U64::from(group_id)));
            match jobx_api_client.job.add(&health_job_add_dto).await {
                Ok(ro) => {
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
                Err(e) => bail!("health check job added failed: {e:?}"),
            }

            // DLQ 同步计划（独立计划，不再挂在健康检查里）
            let mut dlq_job_add_dto: jobx_api::dto::JobxJobAddDto =
                dbz_config.dead_letter.sync_job.job.clone().into();
            dlq_job_add_dto.group_id = Some(Some(U64::from(group_id)));
            match jobx_api_client.job.add(&dlq_job_add_dto).await {
                Ok(ro) => {
                    if ro.is_ok() {
                        info!("dead letter sync job added successfully");
                    } else if ro.is_warn_code(RO_CODE_WARNING_DUPLICATE_KEY) {
                        ensure_existing_job_group(
                            &dbz_config.dead_letter.sync_job.job.name,
                            group_id,
                        )
                        .await?;
                        info!("dead letter sync job already exists, group confirmed");
                    } else {
                        bail!("dead letter sync job added failed: {ro:?}");
                    }
                }
                Err(e) => bail!("dead letter sync job added failed: {e:?}"),
            }
        } else {
            set_jobx_executor_config(dbz_config.health_check.executor.clone());
            set_jobx_executor_config(dbz_config.dead_letter.sync_job.executor.clone());
        }
        set_dbz_config(&dbz_config);
    }
    Ok(())
}
