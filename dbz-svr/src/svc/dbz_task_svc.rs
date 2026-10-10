use crate::config::AppConfig;
use crate::dic::ConnectorStatus;
use anyhow::anyhow;
use arc_swap::ArcSwapOption;
use jobx_api::dic::JobType;
use jobx_api::dto::JobxJobAddDto;
use jobx_api_client::api_client::JobxJobApiClient;
use robotech::api_client::ApiClientConfig;
use robotech::macros::svc;
use robotech::micro_svc::FeignApiClient;
use sea_orm::{DatabaseConnection, DatabaseTransaction};
use serde::Deserialize;
use std::sync::Arc;
use tracing::{info, warn};
use wheel_rs::time_utils::now_ms;

static APP_CONFIG: ArcSwapOption<AppConfig> = ArcSwapOption::const_empty();

pub fn set_app_config(config: AppConfig) {
    APP_CONFIG.store(Some(Arc::new(config)));
}

fn get_app_config() -> Result<Arc<AppConfig>, anyhow::Error> {
    APP_CONFIG
        .load_full()
        .ok_or_else(|| anyhow!("AppConfig not initialized"))
}

#[svc]
pub struct DbzTaskSvc;

impl DbzTaskSvc {
    #[db_unwrap(transaction_required)]
    #[log_call]
    pub async fn register<C>(
        add_dto: DbzTaskAddDto,
        #[skip_log] db: Option<&C>,
    ) -> Result<Ro<DbzTaskVo>, SvcError>
    where
        C: ConnectionTrait,
    {
        let task_vo = Self::add(add_dto, Some(db)).await?.extra
            .ok_or_else(|| SvcError::Runtime(anyhow!("创建任务失败")))?;

        if let Err(e) = Self::deploy_connector(&task_vo).await {
            warn!("向 Kafka Connect 注册连接器失败: {e:?}");
        }

        if let Err(e) = Self::create_check_job(&task_vo).await {
            warn!("创建定时检查任务计划失败: {e:?}");
        }

        Ok(Ro::success("注册成功".to_string()).extra(Some(task_vo)))
    }

    #[db_unwrap(transaction_required)]
    #[log_call]
    pub async fn unregister<C>(
        id: U64,
        #[skip_log] db: Option<&C>,
    ) -> Result<Ro<DbzTaskVo>, SvcError>
    where
        C: ConnectionTrait,
    {
        let task_vo = Self::get_by_id(id, Some(db)).await?.extra
            .ok_or_else(|| SvcError::Runtime(anyhow!("任务不存在")))?;

        if let Err(e) = Self::delete_connector(&task_vo).await {
            warn!("删除 Kafka Connect 连接器失败: {e:?}");
        }

        if let Err(e) = Self::delete_check_job(&task_vo).await {
            warn!("删除定时检查任务计划失败: {e:?}");
        }

        let ro = Self::del_by_id(id, Some(db)).await?;
        Ok(ro)
    }

    #[db_unwrap]
    #[log_call]
    pub async fn check_status<C>(
        id: U64,
        #[skip_log] db: Option<&C>,
    ) -> Result<Ro<DbzTaskVo>, SvcError>
    where
        C: ConnectionTrait,
    {
        let task_vo = Self::get_by_id(id, Some(db)).await?.extra
            .ok_or_else(|| SvcError::Runtime(anyhow!("任务不存在")))?;

        let status = Self::fetch_connector_status(&task_vo).await?;

        let modify_dto = DbzTaskModifyDto::builder()
            .id(task_vo.id)
            .status(status)
            .last_check_ms(Some(now_ms() as i64))
            ._current_user_id(task_vo.updator_id)
            .build();

        let updated = Self::modify(modify_dto, Some(db)).await?.extra
            .ok_or_else(|| SvcError::Runtime(anyhow!("更新任务状态失败")))?;

        if status == ConnectorStatus::Failed {
            warn!("连接器 {} 状态为 FAILED，触发告警和重启", task_vo.name);
            let task_name = task_vo.name.clone();
            tokio::spawn(async move {
                if let Err(e) = Self::send_alarm(&task_name, "FAILED").await {
                    warn!("发送告警失败: {e:?}");
                }
            });
            let task_id = task_vo.id;
            tokio::spawn(async move {
                if let Err(e) = Self::restart_connector_inner(task_id).await {
                    warn!("重启连接器失败: {e:?}");
                }
            });
        }

        Ok(Ro::success("状态检查完成".to_string()).extra(Some(updated)))
    }

    #[db_unwrap]
    #[log_call]
    pub async fn restart<C>(
        id: U64,
        #[skip_log] db: Option<&C>,
    ) -> Result<Ro<DbzTaskVo>, SvcError>
    where
        C: ConnectionTrait,
    {
        let task_vo = Self::get_by_id(id, Some(db)).await?.extra
            .ok_or_else(|| SvcError::Runtime(anyhow!("任务不存在")))?;

        Self::deploy_connector(&task_vo).await.map_err(|e| {
            SvcError::Runtime(anyhow!("重启连接器失败: {e}"))
        })?;

        let modify_dto = DbzTaskModifyDto::builder()
            .id(task_vo.id)
            .status(ConnectorStatus::Running)
            .last_check_ms(Some(now_ms() as i64))
            ._current_user_id(task_vo.updator_id)
            .build();

        let updated = Self::modify(modify_dto, Some(db)).await?.extra
            .ok_or_else(|| SvcError::Runtime(anyhow!("更新任务状态失败")))?;

        Ok(Ro::success("重启成功".to_string()).extra(Some(updated)))
    }

    #[log_call]
    pub async fn scan_and_check_status() -> Result<(), anyhow::Error> {
        use crate::mo::dbz_task;
        use sea_orm::ColumnTrait;
        use sea_orm::EntityTrait;
        use sea_orm::QueryFilter;

        let db = robotech::db::get_db_conn()?;

        let tasks = dbz_task::Entity::find()
            .filter(dbz_task::Column::Enabled.eq(true))
            .all(db.as_ref())
            .await?;

        for task in tasks {
            let task_id = U64(task.id as u64);
            if let Err(e) = Self::check_status::<DatabaseConnection>(task_id, Some(db.as_ref())).await {
                warn!("检查任务 {} 状态失败: {e:?}", task.name);
            }
        }

        Ok(())
    }

    // ========== 私有方法 ==========

    async fn deploy_connector(task: &DbzTaskVo) -> Result<(), anyhow::Error> {
        let config: serde_json::Value = task
            .connector_config
            .as_deref()
            .unwrap_or("{}")
            .parse()
            .unwrap_or(serde_json::json!({}));

        let payload = serde_json::json!({
            "name": task.connector_name,
            "config": config,
        });

        let url = format!(
            "{}/connectors",
            task.kafka_connect_url.trim_end_matches('/')
        );

        let client = reqwest::Client::new();
        let resp = client
            .put(&url)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        if !resp.status().is_success() {
            let body = resp.text().await?;
            return Err(anyhow!("Kafka Connect 部署连接器失败: {body}"));
        }

        info!("连接器 {} 部署成功", task.connector_name);
        Ok(())
    }

    async fn delete_connector(task: &DbzTaskVo) -> Result<(), anyhow::Error> {
        let url = format!(
            "{}/connectors/{}",
            task.kafka_connect_url.trim_end_matches('/'),
            task.connector_name
        );

        let client = reqwest::Client::new();
        let resp = client.delete(&url).send().await?;

        if !resp.status().is_success() {
            let body = resp.text().await?;
            return Err(anyhow!("Kafka Connect 删除连接器失败: {body}"));
        }

        info!("连接器 {} 删除成功", task.connector_name);
        Ok(())
    }

    async fn fetch_connector_status(task: &DbzTaskVo) -> Result<ConnectorStatus, SvcError> {
        let url = format!(
            "{}/connectors/{}/status",
            task.kafka_connect_url.trim_end_matches('/'),
            task.connector_name
        );

        let client = reqwest::Client::new();
        let resp = client.get(&url).send().await.map_err(|e| {
            SvcError::Runtime(anyhow!("查询 Kafka Connect 状态失败: {e}"))
        })?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(SvcError::Runtime(anyhow!(
                "查询 Kafka Connect 状态失败: {body}"
            )));
        }

        #[derive(Deserialize)]
        struct ConnectorStatusResponse {
            connector: ConnectorState,
            #[allow(dead_code)]
            tasks: Option<Vec<TaskState>>,
        }

        #[derive(Deserialize)]
        struct ConnectorState {
            state: String,
        }

        #[derive(Deserialize)]
        struct TaskState {
            #[allow(dead_code)]
            state: String,
            #[allow(dead_code)]
            id: Option<i32>,
            #[allow(dead_code)]
            trace: Option<String>,
        }

        let status_resp: ConnectorStatusResponse = resp.json().await.map_err(|e| {
            SvcError::Runtime(anyhow!("解析 Kafka Connect 状态响应失败: {e}"))
        })?;

        let state = match status_resp.connector.state.as_str() {
            "RUNNING" => ConnectorStatus::Running,
            "FAILED" => ConnectorStatus::Failed,
            "PAUSED" => ConnectorStatus::Paused,
            "UNASSIGNED" => ConnectorStatus::Unassigned,
            "DESTROYED" => ConnectorStatus::Destroyed,
            _ => ConnectorStatus::Unassigned,
        };

        Ok(state)
    }

    async fn create_check_job(task: &DbzTaskVo) -> Result<(), anyhow::Error> {
        let config = get_app_config()?;
        let api_config = ApiClientConfig::Simple {
            base_url: config.jobx.base_url.clone(),
            auth: None,
        };
        let feign_client = FeignApiClient::new(api_config).await;
        let job_client = JobxJobApiClient::new(feign_client);

        let interval_ms = task.check_interval;
        let interval_duration = robotech::api::Duration(
            std::time::Duration::from_millis(interval_ms.value()),
        );

        let add_dto = JobxJobAddDto::builder()
            .name(format!("dbz-check-{}", task.name))
            .executor_code("dbz-svr".to_string())
            .job_type(JobType::FixedDelay)
            .cron(None)
            .interval_duration(Some(interval_duration))
            .high_freq(None)
            .report_result(true)
            .remark(Some(format!("定时检查 Debezium 连接器 {} 状态", task.name)))
            ._current_user_id(U64(1))
            .build();

        let result = job_client.add(&add_dto).await?;
        if let Some(job_vo) = result.extra {
            let db = robotech::db::get_db_conn()?;
            let modify_dto = DbzTaskModifyDto::builder()
                .id(task.id)
                .job_id(Some(job_vo.id as i64))
                ._current_user_id(U64(1))
                .build();
            Self::modify::<DatabaseConnection>(modify_dto, Some(db.as_ref())).await?;
        }

        Ok(())
    }

    async fn delete_check_job(task: &DbzTaskVo) -> Result<(), anyhow::Error> {
        if let Some(job_id) = task.job_id {
            let config = get_app_config()?;
            let api_config = ApiClientConfig::Simple {
                base_url: config.jobx.base_url.clone(),
                auth: None,
            };
            let feign_client = FeignApiClient::new(api_config).await;
            let job_client = JobxJobApiClient::new(feign_client);
            job_client.del_by_id(job_id.value(), 0).await?;
        }
        Ok(())
    }

    async fn send_alarm(task_name: &str, status: &str) -> Result<(), anyhow::Error> {
        let config = get_app_config()?;
        warn!(
            "[ALARM] Debezium 连接器 {} 状态异常: {}，alarm 服务地址: {}",
            task_name, status, config.alarm.base_url
        );
        Ok(())
    }

    async fn restart_connector_inner(id: U64) -> Result<(), anyhow::Error> {
        Self::restart::<DatabaseTransaction>(id, None).await?;
        Ok(())
    }
}