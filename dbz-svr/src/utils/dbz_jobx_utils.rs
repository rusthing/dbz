//! # jobx 任务计划工具
//!
//! 通过 jobx-api-client 与 jobx-svr 交互，为 Debezium 实例创建/删除定时检查任务计划。
//! 实例注册时创建计划（间隔取自 `[dbz] check-interval`），注销时删除对应计划。

use crate::config::get_app_config;
use anyhow::anyhow;
use jobx_api::dic::JobType;
use jobx_api::dto::JobxJobAddDto;
use jobx_api_client::api_client::JobxJobApiClient;
use robotech::api::{Duration, U64};
use robotech::api_client::ApiClientConfig;
use robotech::micro_svc::FeignApiClient;
use tracing::info;

/// # jobx 任务计划工具
pub struct DbzJobxUtils;

impl DbzJobxUtils {
    /// # 创建定时检查任务计划
    ///
    /// 以固定延迟（`FixedDelay`）方式创建 jobx 任务计划，任务执行器编码取自
    /// `[dbz] executor-code`，检查间隔取自 `[dbz] check-interval`。
    ///
    /// ## 参数
    /// * `instance_name` - 实例名称，用于生成任务计划名称
    /// * `current_user_id` - 当前操作用户ID
    ///
    /// ## 返回值
    /// * `Ok(u64)` - 创建成功，返回 jobx 任务计划ID
    /// * `Err(anyhow::Error)` - 创建失败
    pub async fn create_check_job(
        instance_name: &str,
        current_user_id: U64,
    ) -> Result<u64, anyhow::Error> {
        let app_config = get_app_config()?;
        let job_client = Self::job_client(&app_config.jobx.base_url).await;

        let add_dto = JobxJobAddDto::builder()
            .name(format!("dbz-check-{instance_name}"))
            .executor_code(app_config.dbz.executor_code.clone())
            .job_type(JobType::FixedDelay)
            .interval_duration(Some(Duration(app_config.dbz.check_interval)))
            .report_result(true)
            .remark(Some(format!(
                "定时检查 Debezium 实例<{instance_name}>的连接器状态"
            )))
            ._current_user_id(current_user_id)
            .build();

        let ro = job_client.add(&add_dto).await?;
        let job_vo = ro
            .extra
            .ok_or_else(|| anyhow!("创建 jobx 任务计划失败: {}", ro.msg))?;
        info!(
            "实例<{instance_name}>的 jobx 任务计划创建成功, job_id={}",
            job_vo.id
        );
        Ok(job_vo.id)
    }

    /// # 删除定时检查任务计划
    ///
    /// ## 参数
    /// * `job_id` - jobx 任务计划ID
    ///
    /// ## 返回值
    /// * `Ok(())` - 删除成功
    /// * `Err(anyhow::Error)` - 删除失败
    pub async fn delete_check_job(job_id: u64) -> Result<(), anyhow::Error> {
        let app_config = get_app_config()?;
        let job_client = Self::job_client(&app_config.jobx.base_url).await;
        job_client.del_by_id(job_id, 1).await?;
        info!("jobx 任务计划删除成功, job_id={job_id}");
        Ok(())
    }

    /// # 构建 jobx 任务计划 API 客户端
    async fn job_client(base_url: &str) -> JobxJobApiClient {
        let api_config = ApiClientConfig::Simple {
            base_url: base_url.to_string(),
            auth: None,
        };
        JobxJobApiClient::new(FeignApiClient::new(api_config).await)
    }
}
