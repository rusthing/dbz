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
        if let Err(status) = Self::ensure_and_check_connector(
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
            return Err(anyhow!("下游连接器异常: {}", status.value()));
        }

        // 3. 检查上游连接器
        if let Err(status) = Self::ensure_and_check_connector(
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
            return Err(anyhow!("上游连接器异常: {}", status.value()));
        }

        // 4. 一切正常
        Self::update_run_status(instance, InstanceRunStatus::Running).await?;
        Ok(())
    }

    /// # 确保连接器已注册并检查其运行状态
    ///
    /// 先检查连接器是否存在，不存在则部署；然后查询其运行状态。
    /// 若检查失败、部署失败或状态异常，返回对应的错误状态。
    ///
    /// ## 参数
    /// * `instance` - 实例信息
    /// * `base_url` - Kafka Connect 地址
    /// * `connector_name` - 连接器名称
    /// * `connector_config` - 连接器配置
    /// * `check_failed_status` - 检查连接器是否存在失败时对应的运行状态
    /// * `register_failed_status` - 部署连接器失败时对应的运行状态
    /// * `status_failed_status` - 状态查询失败时对应的运行状态
    /// * `status_error_status` - 状态已知异常（Failed/Paused/Destroyed）时对应的运行状态
    /// * `check_error_status` - 状态未知/无法识别（Unassigned）时对应的运行状态
    ///
    /// ## 返回值
    /// * `Ok(ConnectorStatus)` - 连接器正常运行
    /// * `Err(InstanceRunStatus)` - 连接器异常，返回对应的运行状态
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
    ) -> Result<ConnectorStatus, InstanceRunStatus> {
        // 检查连接器是否存在，不存在则部署
        let exists = DbzConnectorUtils::connector_exists(base_url, connector_name)
            .await
            .map_err(|e| {
                warn!(
                    "实例<{}>: 检查连接器<{}>是否存在失败: {e}",
                    instance.name, connector_name
                );
                check_failed_status
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
                    register_failed_status
                })?;
            info!(
                "实例<{}>: 连接器<{}>部署成功",
                instance.name, connector_name
            );
        }

        // 查询连接器状态
        let status = DbzConnectorUtils::fetch_connector_status(base_url, connector_name)
            .await
            .map_err(|e| {
                warn!(
                    "实例<{}>: 查询连接器<{}>状态失败: {e}",
                    instance.name, connector_name
                );
                status_failed_status
            })?;

        if status != ConnectorStatus::Running {
            if status == ConnectorStatus::Unassigned {
                warn!(
                    "实例<{}>: 连接器<{}>返回未知状态: {}",
                    instance.name, connector_name, status
                );
                return Err(check_error_status);
            }
            warn!(
                "实例<{}>: 连接器<{}>状态异常: {}",
                instance.name, connector_name, status
            );
            return Err(status_error_status);
        }

        Ok(status)
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