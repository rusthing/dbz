use crate::dic::{ConnectorStatus, InstanceRunStatus};
use crate::utils::{ConnectorError, DbzAlarmUtils, DbzConnectorUtils, DbzJobxUtils};
use anyhow::anyhow;
use robotech::macros::svc;
use sea_orm::DatabaseConnection;
use tracing::{info, warn};
use wheel_rs::time_utils::now_ms;

#[svc]
pub struct DbzInstanceSvc;

impl DbzInstanceSvc {
    /// # 注册实例
    ///
    /// 依次完成以下步骤：
    /// 1. 调用 jobx 创建定时检查任务计划，并回填 `job_id`；
    /// 2. 写入实例记录（失败时回滚已创建的 jobx 任务计划）；
    /// 3. 向 Kafka Connect 注册上游、下游连接器；
    /// 4. 回写实例的运行状态。
    ///
    /// ## 参数
    /// * `add_dto` - 包含实例信息的传输对象（`job_id` 由本方法生成，无需传入）
    /// * `db` - 数据库连接或事务，如果未提供则创建连接及事务
    ///
    /// ## 返回值
    /// * `Ok(Ro<DbzInstanceVo>)` - 注册完成，返回封装了实例Vo的Ro对象
    /// * `Err(SvcError)` - 注册失败
    #[db_unwrap(transaction_required)]
    #[log_call]
    pub async fn register<C>(
        mut add_dto: DbzInstanceAddDto,
        #[skip_log] db: Option<&C>,
    ) -> Result<Ro<DbzInstanceVo>, SvcError>
    where
        C: ConnectionTrait,
    {
        // 实例名称必填，用于生成 jobx 任务计划名称
        let instance_name = add_dto
            .name
            .clone()
            .filter(|name| !name.is_empty())
            .ok_or_else(|| {
                SvcError::Validation(validator::ValidationError::new("实例名称不能为空"))
            })?;
        let current_user_id = add_dto._current_user_id;

        // 1. 创建 jobx 定时检查任务计划，job_id 由 jobx 生成
        let job_id = DbzJobxUtils::create_check_job(&instance_name, current_user_id)
            .await
            .map_err(|e| SvcError::Runtime(anyhow!("创建定时检查任务计划失败: {e}")))?;
        add_dto.job_id = Some(U64(job_id));

        // 2. 写入实例记录，失败时回滚已创建的 jobx 任务计划
        let mut instance_vo = match Self::add(add_dto, Some(db)).await {
            Ok(ro) => ro
                .extra
                .ok_or_else(|| SvcError::Runtime(anyhow!("注册实例失败")))?,
            Err(e) => {
                if let Err(e2) = DbzJobxUtils::delete_check_job(job_id).await {
                    warn!("回滚 jobx 任务计划失败: {e2:?}");
                }
                return Err(e);
            }
        };

        // 3. 向 Kafka Connect 注册上下游连接器
        let run_status = Self::deploy_connectors(&instance_vo).await;

        // 4. 回写运行状态
        instance_vo = Self::update_run_status(
            instance_vo.id,
            run_status,
            instance_vo.updator_id,
            Some(db),
        )
        .await?;

        if run_status == InstanceRunStatus::Running {
            Ok(Ro::success("注册成功".to_string()).extra(Some(instance_vo)))
        } else {
            Ok(Ro::warn(format!("注册完成，但连接器部署异常: {run_status}"))
                .extra(Some(instance_vo)))
        }
    }

    /// # 注销实例
    ///
    /// 依次完成以下步骤：
    /// 1. 删除 Kafka Connect 的上游、下游连接器；
    /// 2. 删除 jobx 定时检查任务计划；
    /// 3. 删除实例记录。
    ///
    /// ## 参数
    /// * `id` - 要注销的实例ID
    /// * `db` - 数据库连接或事务，如果未提供则创建连接及事务
    ///
    /// ## 返回值
    /// * `Ok(Ro<DbzInstanceVo>)` - 注销成功，返回被删除实例的Vo
    /// * `Err(SvcError)` - 注销失败（实例不存在等）
    #[db_unwrap(transaction_required)]
    #[log_call]
    pub async fn unregister<C>(
        id: U64,
        #[skip_log] db: Option<&C>,
    ) -> Result<Ro<DbzInstanceVo>, SvcError>
    where
        C: ConnectionTrait,
    {
        let instance_vo = Self::get_by_id(id, Some(db))
            .await?
            .extra
            .ok_or_else(|| SvcError::NotFound(format!("id: {id}")))?;

        // 1. 删除 Kafka Connect 上下游连接器（失败仅告警，不阻塞注销）
        if let Err(e) = DbzConnectorUtils::delete_connector(
            &instance_vo.kafka_connect_url,
            &instance_vo.sink_connector_name,
        )
        .await
        {
            warn!("删除下游连接器失败: {e:?}");
        }
        if let Err(e) = DbzConnectorUtils::delete_connector(
            &instance_vo.kafka_connect_url,
            &instance_vo.source_connector_name,
        )
        .await
        {
            warn!("删除上游连接器失败: {e:?}");
        }

        // 2. 删除 jobx 定时检查任务计划
        if let Err(e) = DbzJobxUtils::delete_check_job(instance_vo.job_id.value()).await {
            warn!("删除 jobx 任务计划失败: {e:?}");
        }

        // 3. 删除实例记录
        Self::del_by_id(id, Some(db)).await
    }

    /// # 查询实例连接器状态
    ///
    /// 通过 Kafka Connect REST API 查询上游、下游连接器的实际运行状态，回写实例的
    /// 运行状态；当状态异常时触发告警，并尝试自动重启连接器。
    ///
    /// ## 参数
    /// * `id` - 要检查的实例ID
    /// * `db` - 数据库连接，如果未提供则使用全局数据库连接
    ///
    /// ## 返回值
    /// * `Ok(Ro<DbzInstanceVo>)` - 检查完成，返回更新后的实例Vo
    /// * `Err(SvcError)` - 检查失败（实例不存在等）
    #[db_unwrap]
    #[log_call]
    pub async fn check_status<C>(
        id: U64,
        #[skip_log] db: Option<&C>,
    ) -> Result<Ro<DbzInstanceVo>, SvcError>
    where
        C: ConnectionTrait,
    {
        let instance_vo = Self::get_by_id(id, Some(db))
            .await?
            .extra
            .ok_or_else(|| SvcError::NotFound(format!("id: {id}")))?;

        // 查询上下游连接器状态
        let run_status = Self::check_connectors(&instance_vo).await;
        let updated = Self::update_run_status(id, run_status, instance_vo.updator_id, Some(db)).await?;

        if run_status == InstanceRunStatus::Running {
            return Ok(Ro::success("状态检查完成".to_string()).extra(Some(updated)));
        }

        // 连接器异常：先上报告警，再尝试自动重启
        if let Err(e) = DbzAlarmUtils::send_connector_alarm(&updated, run_status).await {
            warn!("上报连接器异常告警失败: {e:?}");
        }

        let restart_status = Self::deploy_connectors(&updated).await;
        if restart_status == InstanceRunStatus::Running {
            info!("实例<{}>连接器异常，已自动重启", updated.name);
            let restarted =
                Self::update_run_status(id, restart_status, updated.updator_id, Some(db)).await?;
            return Ok(Ro::warn("检测到连接器异常，已自动重启".to_string()).extra(Some(restarted)));
        }

        warn!("实例<{}>连接器自动重启失败: {restart_status}", updated.name);
        Ok(Ro::warn(format!("检测到连接器异常，自动重启失败: {restart_status}"))
            .extra(Some(updated)))
    }

    /// # 重启实例连接器
    ///
    /// 重新向 Kafka Connect 部署上游、下游连接器，并回写运行状态。
    ///
    /// ## 参数
    /// * `id` - 要重启的实例ID
    /// * `db` - 数据库连接，如果未提供则使用全局数据库连接
    ///
    /// ## 返回值
    /// * `Ok(Ro<DbzInstanceVo>)` - 重启完成，返回更新后的实例Vo
    /// * `Err(SvcError)` - 重启失败（实例不存在等）
    #[db_unwrap]
    #[log_call]
    pub async fn restart<C>(
        id: U64,
        #[skip_log] db: Option<&C>,
    ) -> Result<Ro<DbzInstanceVo>, SvcError>
    where
        C: ConnectionTrait,
    {
        let instance_vo = Self::get_by_id(id, Some(db))
            .await?
            .extra
            .ok_or_else(|| SvcError::NotFound(format!("id: {id}")))?;

        let run_status = Self::deploy_connectors(&instance_vo).await;
        let updated = Self::update_run_status(id, run_status, instance_vo.updator_id, Some(db)).await?;

        if run_status == InstanceRunStatus::Running {
            Ok(Ro::success("重启成功".to_string()).extra(Some(updated)))
        } else {
            Ok(Ro::warn(format!("重启失败: {run_status}")).extra(Some(updated)))
        }
    }

    /// # 扫描并检查所有已启用实例的状态
    ///
    /// 由内置调度器周期调用，逐个检查已启用实例的连接器状态。
    ///
    /// ## 返回值
    /// * `Ok(())` - 扫描完成
    /// * `Err(SvcError)` - 扫描失败
    pub async fn scan_and_check_status() -> Result<(), SvcError> {
        let query_dto = DbzInstanceQueryDto::builder().enabled(true).build();
        let instances = Self::list_by_query_dto::<DatabaseConnection>(query_dto, None)
            .await?
            .extra
            .unwrap_or_default();

        for instance in instances {
            if let Err(e) = Self::check_status::<DatabaseConnection>(instance.id, None).await {
                warn!("检查实例<{}>状态失败: {e:?}", instance.name);
            }
        }
        Ok(())
    }

    /// # 回写实例的运行状态
    ///
    /// ## 参数
    /// * `id` - 实例ID
    /// * `run_status` - 新的运行状态
    /// * `current_user_id` - 当前操作用户ID
    /// * `db` - 数据库连接
    ///
    /// ## 返回值
    /// * `Ok(DbzInstanceVo)` - 更新后的实例Vo
    /// * `Err(SvcError)` - 更新失败
    async fn update_run_status<C>(
        id: U64,
        run_status: InstanceRunStatus,
        current_user_id: U64,
        db: Option<&C>,
    ) -> Result<DbzInstanceVo, SvcError>
    where
        C: ConnectionTrait,
    {
        let modify_dto = DbzInstanceModifyDto::builder()
            .id(id)
            .run_status(run_status)
            ._current_user_id(current_user_id)
            ._current_ms(U64(now_ms()))
            .build();
        Self::modify(modify_dto, db)
            .await?
            .extra
            .ok_or_else(|| SvcError::Runtime(anyhow!("更新实例运行状态失败")))
    }

    /// # 向 Kafka Connect 部署上下游连接器
    ///
    /// 先探测 Kafka Connect 连通性，再依次部署下游、上游连接器。
    ///
    /// ## 参数
    /// * `instance` - 实例Vo
    ///
    /// ## 返回值
    /// 返回部署后的运行状态：全部成功为「运行中」，否则返回对应的失败状态。
    async fn deploy_connectors(instance: &DbzInstanceVo) -> InstanceRunStatus {
        // 探测 Kafka Connect 连通性
        if let Err(e) = DbzConnectorUtils::check_connect(&instance.kafka_connect_url).await {
            warn!("实例<{}>连接 Kafka Connect 失败: {e:?}", instance.name);
            return InstanceRunStatus::KafkaConnectFailed;
        }

        // 注册下游连接器
        if let Err(e) = DbzConnectorUtils::deploy_connector(
            &instance.kafka_connect_url,
            &instance.sink_connector_name,
            &instance.sink_connector_config,
        )
        .await
        {
            warn!(
                "实例<{}>注册下游连接器<{}>失败: {e:?}",
                instance.name, instance.sink_connector_name
            );
            return InstanceRunStatus::RegisterSinkFailed;
        }

        // 注册上游连接器
        if let Err(e) = DbzConnectorUtils::deploy_connector(
            &instance.kafka_connect_url,
            &instance.source_connector_name,
            &instance.source_connector_config,
        )
        .await
        {
            warn!(
                "实例<{}>注册上游连接器<{}>失败: {e:?}",
                instance.name, instance.source_connector_name
            );
            return InstanceRunStatus::RegisterSourceFailed;
        }

        InstanceRunStatus::Running
    }

    /// # 检测上下游连接器状态
    ///
    /// 依次检测上游、下游连接器的状态，并根据失败原因映射为不同的运行状态：
    /// - 请求失败（网络/非2xx） -> 检测连接器失败
    /// - 连接器不存在（404）    -> 检测连接器异常
    /// - 状态响应解析失败       -> 检测连接器状态失败
    /// - 状态非 RUNNING         -> 检测连接器状态异常
    ///
    /// ## 参数
    /// * `instance` - 实例Vo
    ///
    /// ## 返回值
    /// 上下游连接器均正常时返回「运行中」，否则返回对应的异常状态。
    async fn check_connectors(instance: &DbzInstanceVo) -> InstanceRunStatus {
        // 检测上游连接器
        match DbzConnectorUtils::fetch_connector_status(
            &instance.kafka_connect_url,
            &instance.source_connector_name,
        )
        .await
        {
            Err(ConnectorError::Request(e)) => {
                warn!("实例<{}>检测上游连接器失败: {e}", instance.name);
                return InstanceRunStatus::CheckSourceFailed;
            }
            Err(ConnectorError::NotFound) => {
                warn!("实例<{}>上游连接器不存在", instance.name);
                return InstanceRunStatus::CheckSourceError;
            }
            Err(ConnectorError::Parse(e)) => {
                warn!("实例<{}>检测上游连接器状态失败: {e}", instance.name);
                return InstanceRunStatus::CheckSourceStatusFailed;
            }
            Ok(status) if status != ConnectorStatus::Running => {
                warn!("实例<{}>上游连接器状态异常: {status}", instance.name);
                return InstanceRunStatus::CheckSourceStatusError;
            }
            Ok(_) => {}
        }

        // 检测下游连接器
        match DbzConnectorUtils::fetch_connector_status(
            &instance.kafka_connect_url,
            &instance.sink_connector_name,
        )
        .await
        {
            Err(ConnectorError::Request(e)) => {
                warn!("实例<{}>检测下游连接器失败: {e}", instance.name);
                return InstanceRunStatus::CheckSinkFailed;
            }
            Err(ConnectorError::NotFound) => {
                warn!("实例<{}>下游连接器不存在", instance.name);
                return InstanceRunStatus::CheckSinkError;
            }
            Err(ConnectorError::Parse(e)) => {
                warn!("实例<{}>检测下游连接器状态失败: {e}", instance.name);
                return InstanceRunStatus::CheckSinkStatusFailed;
            }
            Ok(status) if status != ConnectorStatus::Running => {
                warn!("实例<{}>下游连接器状态异常: {status}", instance.name);
                return InstanceRunStatus::CheckSinkStatusError;
            }
            Ok(_) => {}
        }

        InstanceRunStatus::Running
    }
}
