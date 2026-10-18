use crate::dic::{ConnectorStatus, InstanceRunStatus};
use crate::dto::{DbzInstanceModifyDto, DbzInstanceQueryDto};
use crate::utils::{DbzAlarmUtils, DbzConnectorUtils};
use anyhow::anyhow;
use robotech::db::get_db_conn;
use robotech::macros::svc;
use sea_orm::DatabaseConnection;
use tracing::{info, warn};
use wheel_rs::time_utils::now_ms;

#[svc(after_modify = DbzInstanceSvc::after_modify_hook)]
pub struct DbzInstanceSvc;

/// # 连接器自动修复动作
///
/// 检查出异常时顺带执行一次，只是发请求、不等待生效，结果由下一轮检查确认。
/// 只覆盖**修复手段明确**的故障：连接器 FAILED / UNASSIGNED、任务 FAILED / UNASSIGNED。
/// PAUSED 通常是人工维护、DESTROYED 只能删了重建，都不自动动。
enum RepairAction {
    /// 无需修复，只告警
    None,
    /// 重启连接器（连同其任务），用于连接器自身 FAILED / UNASSIGNED
    RestartConnector,
    /// 只重启这几个异常任务，用于连接器 RUNNING 但任务挂了
    RestartTasks(Vec<u32>),
}

impl DbzInstanceSvc {
    /// # 修改后钩子：实例被禁用时注销连接器
    ///
    /// 由 `#[svc]` 宏在 `modify` 方法执行后自动调用。
    /// 若实例被禁用（`modify_dto.enabled = Some(false)`），注销上游和下游连接器。
    ///
    /// 注：连接器名称变更场景中旧名称无法在 after_modify 中检测（数据库已更新），
    /// 若需处理名称变更，应先禁用旧实例再创建新实例。
    async fn after_modify_hook(
        modify_dto: DbzInstanceModifyDto,
        one: DbzInstanceVo,
    ) -> Result<DbzInstanceVo, SvcError> {
        if modify_dto.enabled == Some(false) {
            info!(
                "实例<{}>(id={}): 被禁用，注销上游连接器<{}>和下游连接器<{}>",
                one.name, one.id, one.source_connector_name, one.sink_connector_name
            );
            DbzConnectorUtils::delete_connector(&one.kafka_connect_url, &one.source_connector_name)
                .await?;
            DbzConnectorUtils::delete_connector(&one.kafka_connect_url, &one.sink_connector_name)
                .await?;
        }

        Ok(one)
    }

    /// # 定时检查所有已启用实例的连接器状态
    ///
    /// 由 jobx 健康检查任务定时触发。遍历所有 `enabled = true` 的实例，
    /// 逐一检查其上游和下游连接器的运行状态，并将结果回写到数据库。
    ///
    /// 单个实例检查失败不会中断整体遍历。
    ///
    /// ## 返回值
    /// - `Ok(Vec<String>)` - 返回所有异常实例的错误详情列表；若无异常则返回空 Vec
    /// - `Err` - 基础设施错误（如数据库连接失败）
    pub async fn check_all_connectors() -> anyhow::Result<String> {
        info!("开始定时检查所有 Debezium 实例的连接器状态...");

        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;

        let query_dto = DbzInstanceQueryDto::builder().enabled(true).build();

        let ro = Self::list_by_query_dto::<DatabaseConnection>(query_dto, Some(&db))
            .await
            .map_err(|e| anyhow!("查询已启用实例列表失败: {e}"))?;

        let instances = ro.extra.unwrap_or_default();
        info!("共找到 {} 个已启用的实例待检查", instances.len());

        let mut errors: Vec<String> = Vec::new();

        for instance in &instances {
            if let Err(e) = Self::check_one_instance(instance).await {
                warn!("检查实例<{}>(id={})失败: {e}", instance.name, instance.id);
                errors.push(format!("实例<{}>(id={}): {e}", instance.name, instance.id));
            }
        }

        let exec_detail = if errors.is_empty() {
            let msg = "所有实例连接器状态检查完成，一切正常";
            info!("{}", msg);
            msg.to_string()
        } else {
            let msg = format!("实例连接器状态检查完成，{} 个实例存在异常", errors.len());
            warn!("{}", msg);
            msg.to_string()
        };

        Ok(exec_detail)
    }

    /// # 检查单个实例的连接器状态
    ///
    /// 按以下顺序检查，任一步骤失败则存库后立即返回：
    /// 1. 探测 Kafka Connect 连通性
    /// 2. 检查下游连接器
    /// 3. 检查上游连接器
    async fn check_one_instance(instance: &DbzInstanceVo) -> Result<(), anyhow::Error> {
        let base_url = &instance.kafka_connect_url;

        // 1. 探测 Kafka Connect 连通性
        if let Err(e) = DbzConnectorUtils::check_connect(base_url).await {
            warn!("实例<{}>: Kafka Connect 连接失败: {e}", instance.name);
            Self::update_run_status(instance, InstanceRunStatus::KafkaConnectFailed).await?;
            DbzAlarmUtils::send_connector_alarm(instance, InstanceRunStatus::KafkaConnectFailed)
                .await?;
            return Err(anyhow!("Kafka Connect 连接失败: {e}"));
        }

        // 2. 检查下游连接器
        if let Err((status, action)) = Self::ensure_and_check_connector(
            instance,
            base_url,
            &instance.sink_connector_name,
            &instance.sink_connector_config,
            InstanceRunStatus::CheckSinkFailed,
            InstanceRunStatus::RegisterSinkFailed,
            InstanceRunStatus::CheckSinkStatusFailed,
            InstanceRunStatus::CheckSinkStatusError,
            InstanceRunStatus::CheckSinkError,
        )
        .await
        {
            Self::update_run_status(instance, status).await?;
            DbzAlarmUtils::send_connector_alarm(instance, status).await?;
            // 告警先发，再修复：自动修复不替代报警
            Self::apply_repair(base_url, &instance.sink_connector_name, action).await;
            return Err(anyhow!("下游连接器异常: {}", status.value()));
        }

        // 3. 检查上游连接器
        if let Err((status, action)) = Self::ensure_and_check_connector(
            instance,
            base_url,
            &instance.source_connector_name,
            &instance.source_connector_config,
            InstanceRunStatus::CheckSourceFailed,
            InstanceRunStatus::RegisterSourceFailed,
            InstanceRunStatus::CheckSourceStatusFailed,
            InstanceRunStatus::CheckSourceStatusError,
            InstanceRunStatus::CheckSourceError,
        )
        .await
        {
            Self::update_run_status(instance, status).await?;
            DbzAlarmUtils::send_connector_alarm(instance, status).await?;
            // 告警先发，再修复：自动修复不替代报警
            Self::apply_repair(base_url, &instance.source_connector_name, action).await;
            return Err(anyhow!("上游连接器异常: {}", status.value()));
        }

        // 4. 一切正常
        Self::update_run_status(instance, InstanceRunStatus::Running).await?;
        Ok(())
    }

    /// # 确保连接器已注册并检查其运行状态
    ///
    /// 先检查连接器是否存在，不存在则部署；然后查询连接器与任务的状态。
    /// 若检查失败、部署失败或状态异常，返回对应的错误状态和修复动作。
    ///
    /// ## 参数
    /// * `instance` - 实例信息
    /// * `base_url` - Kafka Connect 地址
    /// * `connector_name` - 连接器名称
    /// * `connector_config` - 连接器配置
    /// * `check_failed_status` - 检查连接器是否存在失败时对应的运行状态
    /// * `register_failed_status` - 部署连接器失败时对应的运行状态
    /// * `status_failed_status` - 状态查询失败时对应的运行状态
    /// * `status_error_status` - 状态已知异常（Failed/Paused/Destroyed、任务异常）时对应的运行状态
    /// * `check_error_status` - 状态未知/无法识别（Unassigned）时对应的运行状态
    ///
    /// ## 返回值
    /// * `Ok(ConnectorStatus)` - 连接器及其任务都正常运行
    /// * `Err((InstanceRunStatus, RepairAction))` - 连接器异常，返回对应的运行状态与修复动作
    async fn ensure_and_check_connector(
        instance: &DbzInstanceVo,
        base_url: &str,
        connector_name: &str,
        connector_config: &str,
        check_failed_status: InstanceRunStatus,
        register_failed_status: InstanceRunStatus,
        status_failed_status: InstanceRunStatus,
        status_error_status: InstanceRunStatus,
        check_error_status: InstanceRunStatus,
    ) -> Result<ConnectorStatus, (InstanceRunStatus, RepairAction)> {
        // 检查连接器是否存在，不存在则部署
        let exists = DbzConnectorUtils::connector_exists(base_url, connector_name)
            .await
            .map_err(|e| {
                warn!(
                    "实例<{}>: 检查连接器<{}>是否存在失败: {e}",
                    instance.name, connector_name
                );
                (check_failed_status, RepairAction::None)
            })?;

        if !exists {
            info!(
                "实例<{}>: 连接器<{}>不存在，开始部署...",
                instance.name, connector_name
            );
            DbzConnectorUtils::deploy_connector(base_url, connector_name, connector_config)
                .await
                .map_err(|e| {
                    warn!(
                        "实例<{}>: 部署连接器<{}>失败: {e}",
                        instance.name, connector_name
                    );
                    (register_failed_status, RepairAction::None)
                })?;
            info!(
                "实例<{}>: 连接器<{}>部署成功",
                instance.name, connector_name
            );
        }

        // 查询连接器状态（含任务状态）
        let (status, failed_task_ids) =
            DbzConnectorUtils::fetch_connector_status(base_url, connector_name)
                .await
                .map_err(|e| {
                    warn!(
                        "实例<{}>: 查询连接器<{}>状态失败: {e}",
                        instance.name, connector_name
                    );
                    (status_failed_status, RepairAction::None)
                })?;

        if status != ConnectorStatus::Running {
            // 连接器自身 RUNNING、任务挂了：只重启这几个任务
            if !failed_task_ids.is_empty() {
                warn!(
                    "实例<{}>: 连接器<{}>有 {} 个任务异常: {:?}",
                    instance.name,
                    connector_name,
                    failed_task_ids.len(),
                    failed_task_ids
                );
                return Err((
                    status_error_status,
                    RepairAction::RestartTasks(failed_task_ids),
                ));
            }
            if status == ConnectorStatus::Unassigned {
                warn!(
                    "实例<{}>: 连接器<{}>返回未知状态: {}",
                    instance.name, connector_name, status
                );
                return Err((check_error_status, RepairAction::RestartConnector));
            }
            warn!(
                "实例<{}>: 连接器<{}>状态异常: {}",
                instance.name, connector_name, status
            );
            // 只有 failed 有确定的修复动作；paused 多为人工维护、destroyed 只能重建，都不动
            let action = if status == ConnectorStatus::Failed {
                RepairAction::RestartConnector
            } else {
                RepairAction::None
            };
            return Err((status_error_status, action));
        }

        Ok(status)
    }

    /// # 执行自动修复
    ///
    /// 由 [`ensure_and_check_connector`](Self::ensure_and_check_connector) 诊断出的
    /// 修复动作在这里执行。调用方已经先回写状态、发出告警，修复不会替代报警。
    ///
    /// 修复只是发请求：Kafka Connect 的 restart 是异步的，这里不等待生效，
    /// 结果由下一轮检查确认；请求失败也只记日志，下一轮检查会重新诊断。
    ///
    /// ## 参数
    /// * `base_url` - Kafka Connect 地址
    /// * `connector_name` - 连接器名称
    /// * `action` - 修复动作
    async fn apply_repair(base_url: &str, connector_name: &str, action: RepairAction) {
        match action {
            RepairAction::None => {}
            RepairAction::RestartConnector => {
                info!("连接器<{connector_name}>状态异常，自动重启连接器");
                // 连接器自身异常，连任务一起重启
                if let Err(e) =
                    DbzConnectorUtils::restart_connector(base_url, connector_name, true, false)
                        .await
                {
                    warn!("连接器<{connector_name}>自动重启失败: {e}");
                }
            }
            RepairAction::RestartTasks(task_ids) => {
                info!(
                    "连接器<{connector_name}>有 {} 个任务异常，自动重启: {task_ids:?}",
                    task_ids.len()
                );
                for task_id in task_ids {
                    if let Err(e) =
                        DbzConnectorUtils::restart_connector_task(base_url, connector_name, task_id)
                            .await
                    {
                        warn!("连接器<{connector_name}>的任务<{task_id}>自动重启失败: {e}");
                    }
                }
            }
        }
    }

    /// # 更新实例运行状态
    ///
    /// 通过 SVC 的 modify 方法将最新的运行状态回写到数据库。
    async fn update_run_status(
        instance: &DbzInstanceVo,
        run_status: InstanceRunStatus,
    ) -> Result<(), anyhow::Error> {
        let modify_dto = DbzInstanceModifyDto::builder()
            .id(instance.id)
            .run_status(run_status)
            ._current_user_id(U64(1))
            ._current_ms(U64(now_ms()))
            .build();

        Self::modify::<DatabaseConnection>(modify_dto, None)
            .await
            .map_err(|e| anyhow!("更新实例<{}>运行状态失败: {e}", instance.name))?;

        info!(
            "实例<{}>运行状态已更新: {}",
            instance.name,
            run_status.value()
        );
        Ok(())
    }
}
