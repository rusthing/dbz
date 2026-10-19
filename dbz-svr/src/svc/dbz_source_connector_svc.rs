//! # 上游连接器服务
//!
//! 上游（source）连接器是 Debezium 从源库读取变更并写入 Kafka 的连接器；一条
//! `dbz_source_connector` 记录对应 Kafka Connect 上的一个连接器实例，并保存该
//! Kafka Connect 的地址。下游（sink）连接器通过 `source_connector_id` 归属于它，
//! 见 [`DbzSinkConnectorSvc`](crate::svc::DbzSinkConnectorSvc)。
//!
//! 健康检查以「上游连接器」为入口：先探测 Kafka Connect 连通性，再检查上游连接器
//! 本身，最后逐个检查挂在该上游下的下游连接器，各自回写各自的运行状态、各自告警。

use crate::dic::{ConnectorRunStatus, ConnectorStatus};
use crate::dto::DbzSinkConnectorQueryDto;
use crate::svc::DbzSinkConnectorSvc;
use crate::utils::{DbzAlarmUtils, DbzConnectorUtils};
use crate::vo::DbzSinkConnectorVo;
use anyhow::anyhow;
use robotech::db::get_db_conn;
use robotech::macros::svc;
use sea_orm::DatabaseConnection;
use std::collections::HashMap;
use tracing::{info, warn};
use wheel_rs::time_utils::now_ms;

#[svc(
    before_del_by_id = DbzSourceConnectorSvc::before_del_by_id_hook,
    after_modify = DbzSourceConnectorSvc::after_modify_hook
)]
pub struct DbzSourceConnectorSvc;

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

/// # 待检查的连接器
///
/// 上游与下游连接器的检查流程完全一致，差别只在身份（日志里的标签）与
/// Kafka Connect 地址的来源，因此抽成同一组入参。
struct ConnectorCheck<'a> {
    /// 日志里标识这个连接器，如 `上游连接器<mysql-src>(id=1)`
    label: &'a str,
    /// Kafka Connect 地址
    base_url: &'a str,
    /// 连接器名称
    connector_name: &'a str,
    /// 连接器配置（JSON）
    connector_config: &'a str,
}

impl DbzSourceConnectorSvc {
    /// # 删除前钩子：上游下还有下游连接器时拒绝删除
    ///
    /// 数据库侧已经是 `on delete restrict`，这里提前拦一道，把外键错误换成
    /// 可读的提示，避免调用方拿到一条裸的 SQL 报错。
    ///
    /// 下游是否启用不影响判断：外键约束不看 `enabled`。
    ///
    /// ## 参数
    /// * `id` - 待删除的上游连接器ID
    ///
    /// ## 返回值
    /// * `Ok(U64)` - 可以删除，原样返回 ID
    /// * `Err(SvcError)` - 该上游下仍有下游连接器
    async fn before_del_by_id_hook(id: U64) -> Result<U64, SvcError> {
        let query_dto = DbzSinkConnectorQueryDto::builder()
            .source_connector_id(id)
            .build();
        let ro =
            DbzSinkConnectorSvc::list_by_query_dto::<DatabaseConnection>(query_dto, None).await?;
        let sink_count = ro.extra.unwrap_or_default().len();
        if sink_count > 0 {
            return Err(anyhow!(
                "上游连接器(id={})下还有 {sink_count} 个下游连接器，请先删除或改挂这些下游连接器",
                id.value()
            )
            .into());
        }
        Ok(id)
    }

    /// # 修改后钩子：上游连接器被禁用时注销连接器
    ///
    /// 由 `#[svc]` 宏在 `modify` 方法执行后自动调用。
    /// 若记录被禁用（`modify_dto.enabled = Some(false)`），注销该连接器。
    ///
    /// 注：不级联处理下游连接器 —— 它们各自有 `enabled` 开关，健康检查也只在
    /// 上游启用时才检查其下游，因此禁用上游后下游不会再被检查；要彻底停止下游，
    /// 请单独禁用对应的下游连接器。
    ///
    /// 注：连接器名称变更场景中旧名称无法在 after_modify 中检测（数据库已更新），
    /// 若需处理名称变更，应先禁用旧记录再创建新记录。
    async fn after_modify_hook(
        modify_dto: DbzSourceConnectorModifyDto,
        one: DbzSourceConnectorVo,
    ) -> Result<DbzSourceConnectorVo, SvcError> {
        if modify_dto.enabled == Some(false) {
            info!(
                "上游连接器<{}>(id={}): 被禁用，注销连接器<{}>",
                one.name, one.id, one.connector_name
            );
            DbzConnectorUtils::delete_connector(&one.kafka_connect_url, &one.connector_name)
                .await?;
        }

        Ok(one)
    }

    /// # 取上游连接器配置的 Kafka Connect 地址
    ///
    /// 下游连接器与连接器运维接口都不保存地址，统一按上游连接器取。
    ///
    /// ## 参数
    /// * `id` - 上游连接器ID
    ///
    /// ## 返回值
    /// * `Ok(String)` - 去掉首尾空白与末尾 `/` 的 Kafka Connect 地址
    /// * `Err(anyhow::Error)` - 上游连接器不存在或未配置地址
    pub(crate) async fn kafka_connect_url(id: U64) -> Result<String, anyhow::Error> {
        let one = Self::get_by_id::<DatabaseConnection>(id, None)
            .await
            .map_err(|e| anyhow!("查询上游连接器<{}>失败: {e}", id.value()))?
            .extra
            .ok_or_else(|| anyhow!("上游连接器<{}>不存在", id.value()))?;

        let url = one
            .kafka_connect_url
            .trim()
            .trim_end_matches('/')
            .to_string();
        if url.is_empty() {
            return Err(anyhow!("上游连接器<{}>未配置 Kafka Connect 地址", one.name));
        }
        Ok(url)
    }

    /// # 定时检查所有上游连接器及其下游连接器
    ///
    /// 由 jobx 健康检查任务定时触发。遍历所有 `enabled = true` 的上游连接器，
    /// 逐一检查 Kafka Connect 连通性、上游连接器本身，以及挂在该上游下的每个
    /// 已启用下游连接器，并将各自的运行状态回写到各自的表。
    ///
    /// 单个连接器检查失败不会中断整体遍历；上游 Kafka Connect 不可达时只回写
    /// 上游的状态并跳过其下游（原因相同，不重复刷状态与告警）。
    ///
    /// ## 返回值
    /// * `Ok(String)` - 本次检查的执行摘要
    /// * `Err` - 基础设施错误（如数据库连接失败）
    pub async fn check_all_connectors() -> anyhow::Result<String> {
        info!("开始定时检查所有上游连接器及其下游连接器的状态...");

        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;

        // 上游连接器是检查的入口：Kafka Connect 地址与启用开关都在它身上
        let source_ro = Self::list_by_query_dto::<DatabaseConnection>(
            DbzSourceConnectorQueryDto::builder().enabled(true).build(),
            Some(&db),
        )
        .await
        .map_err(|e| anyhow!("查询已启用的上游连接器列表失败: {e}"))?;
        let sources = source_ro.extra.unwrap_or_default();

        // 下游连接器一次全取回再按上游分组，避免逐个上游查库
        let sink_ro = DbzSinkConnectorSvc::list_by_query_dto::<DatabaseConnection>(
            DbzSinkConnectorQueryDto::builder().enabled(true).build(),
            Some(&db),
        )
        .await
        .map_err(|e| anyhow!("查询已启用的下游连接器列表失败: {e}"))?;
        let mut sinks_by_source: HashMap<u64, Vec<DbzSinkConnectorVo>> = HashMap::new();
        for sink in sink_ro.extra.unwrap_or_default() {
            sinks_by_source
                .entry(sink.source_connector_id.value())
                .or_default()
                .push(sink);
        }

        let sink_count: usize = sinks_by_source.values().map(Vec::len).sum();
        info!(
            "共找到 {} 个已启用的上游连接器、{} 个已启用的下游连接器，开始检查",
            sources.len(),
            sink_count
        );

        let mut errors: Vec<String> = Vec::new();

        for source in &sources {
            let empty: Vec<DbzSinkConnectorVo> = Vec::new();
            let sinks = sinks_by_source.get(&source.id.value()).unwrap_or(&empty);
            match Self::check_one_source(source, sinks).await {
                Ok(mut connector_errors) => errors.append(&mut connector_errors),
                Err(e) => {
                    warn!(
                        "检查上游连接器<{}>(id={})失败: {e}",
                        source.name,
                        source.id.value()
                    );
                    errors.push(format!(
                        "上游连接器<{}>(id={}): {e}",
                        source.name,
                        source.id.value()
                    ));
                }
            }
        }

        let exec_detail = if errors.is_empty() {
            let msg = "所有连接器状态检查完成，一切正常";
            info!("{}", msg);
            msg.to_string()
        } else {
            let msg = format!("连接器状态检查完成，{} 个连接器存在异常", errors.len());
            warn!("{}", msg);
            msg
        };

        Ok(exec_detail)
    }

    /// # 检查一个上游连接器及其下游连接器
    ///
    /// 按以下顺序检查，各自回写状态；上游 Kafka Connect 不可达时提前返回。
    ///
    /// ## 返回值
    /// * `Ok(Vec<String>)` - 异常连接器的详情列表；全部正常时为空
    /// * `Err(anyhow::Error)` - 基础设施错误（数据库读写失败、Kafka Connect 不可达）
    async fn check_one_source(
        source: &DbzSourceConnectorVo,
        sinks: &[DbzSinkConnectorVo],
    ) -> Result<Vec<String>, anyhow::Error> {
        let base_url = source.kafka_connect_url.trim().trim_end_matches('/');

        // 1. 探测 Kafka Connect 连通性。地址不通时这台 Kafka Connect 上的连接器
        //    都查不了，只回写上游状态：把同一个原因刷到每条下游记录上只会制造噪音。
        if let Err(e) = DbzConnectorUtils::check_connect(base_url).await {
            warn!(
                "上游连接器<{}>(id={}): Kafka Connect 连接失败: {e}",
                source.name,
                source.id.value()
            );
            Self::update_run_status(source, ConnectorRunStatus::KafkaConnectFailed).await?;
            DbzAlarmUtils::send_source_connector_alarm(
                source,
                ConnectorRunStatus::KafkaConnectFailed,
            )
            .await?;
            return Err(anyhow!("Kafka Connect 连接失败: {e}"));
        }

        let mut errors: Vec<String> = Vec::new();

        // 2. 上游连接器
        let label = format!("上游连接器<{}>(id={})", source.name, source.id.value());
        let check = ConnectorCheck {
            label: &label,
            base_url,
            connector_name: &source.connector_name,
            connector_config: &source.connector_config,
        };
        match Self::ensure_and_check_connector(&check).await {
            Ok(_) => Self::update_run_status(source, ConnectorRunStatus::Running).await?,
            Err((run_status, action)) => {
                Self::update_run_status(source, run_status).await?;
                DbzAlarmUtils::send_source_connector_alarm(source, run_status).await?;
                // 告警先发，再修复：自动修复不替代报警
                Self::apply_repair(base_url, &source.connector_name, action).await;
                errors.push(format!("{label}: {}", run_status.value()));
            }
        }

        // 3. 下游连接器
        for sink in sinks {
            let label = format!(
                "下游连接器<{}>(id={}, 上游<{}>)",
                sink.name,
                sink.id.value(),
                source.name
            );
            let check = ConnectorCheck {
                label: &label,
                base_url,
                connector_name: &sink.connector_name,
                connector_config: &sink.connector_config,
            };
            match Self::ensure_and_check_connector(&check).await {
                Ok(_) => {
                    DbzSinkConnectorSvc::update_run_status(sink, ConnectorRunStatus::Running)
                        .await?
                }
                Err((run_status, action)) => {
                    DbzSinkConnectorSvc::update_run_status(sink, run_status).await?;
                    DbzAlarmUtils::send_sink_connector_alarm(sink, source, run_status).await?;
                    // 告警先发，再修复：自动修复不替代报警
                    Self::apply_repair(base_url, &sink.connector_name, action).await;
                    errors.push(format!("{label}: {}", run_status.value()));
                }
            }
        }

        Ok(errors)
    }

    /// # 确保连接器已注册并检查其状态
    ///
    /// 先检查连接器是否存在，不存在则部署；然后查询连接器与任务的状态。
    /// 若检查失败、部署失败或状态异常，返回对应的错误状态和修复动作。
    ///
    /// ## 返回值
    /// * `Ok(ConnectorStatus)` - 连接器及其任务都正常运行
    /// * `Err((ConnectorRunStatus, RepairAction))` - 连接器异常，返回对应的运行状态与修复动作
    async fn ensure_and_check_connector(
        check: &ConnectorCheck<'_>,
    ) -> Result<ConnectorStatus, (ConnectorRunStatus, RepairAction)> {
        let ConnectorCheck {
            label,
            base_url,
            connector_name,
            connector_config,
        } = *check;

        // 检查连接器是否存在，不存在则部署
        let exists = DbzConnectorUtils::connector_exists(base_url, connector_name)
            .await
            .map_err(|e| {
                warn!("{label}: 检查连接器<{connector_name}>是否存在失败: {e}");
                (ConnectorRunStatus::CheckFailed, RepairAction::None)
            })?;

        if !exists {
            info!("{label}: 连接器<{connector_name}>不存在，开始部署...");
            DbzConnectorUtils::deploy_connector(base_url, connector_name, connector_config)
                .await
                .map_err(|e| {
                    warn!("{label}: 部署连接器<{connector_name}>失败: {e}");
                    (ConnectorRunStatus::RegisterFailed, RepairAction::None)
                })?;
            info!("{label}: 连接器<{connector_name}>部署成功");
        }

        // 查询连接器状态（含任务状态）
        let (status, failed_task_ids) =
            DbzConnectorUtils::fetch_connector_status(base_url, connector_name)
                .await
                .map_err(|e| {
                    warn!("{label}: 查询连接器<{connector_name}>状态失败: {e}");
                    (ConnectorRunStatus::CheckStatusFailed, RepairAction::None)
                })?;

        if status != ConnectorStatus::Running {
            // 连接器自身 RUNNING、任务挂了：只重启这几个任务
            if !failed_task_ids.is_empty() {
                warn!(
                    "{label}: 连接器<{connector_name}>有 {} 个任务异常: {:?}",
                    failed_task_ids.len(),
                    failed_task_ids
                );
                return Err((
                    ConnectorRunStatus::CheckStatusError,
                    RepairAction::RestartTasks(failed_task_ids),
                ));
            }
            if status == ConnectorStatus::Unassigned {
                warn!("{label}: 连接器<{connector_name}>返回未知状态: {status}");
                return Err((
                    ConnectorRunStatus::CheckError,
                    RepairAction::RestartConnector,
                ));
            }
            warn!("{label}: 连接器<{connector_name}>状态异常: {status}");
            // 只有 failed 有确定的修复动作；paused 多为人工维护、destroyed 只能重建，都不动
            let action = if status == ConnectorStatus::Failed {
                RepairAction::RestartConnector
            } else {
                RepairAction::None
            };
            return Err((ConnectorRunStatus::CheckStatusError, action));
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

    /// # 更新上游连接器的运行状态
    ///
    /// 通过 SVC 的 modify 方法将最新的运行状态回写到数据库。
    async fn update_run_status(
        source: &DbzSourceConnectorVo,
        run_status: ConnectorRunStatus,
    ) -> Result<(), anyhow::Error> {
        let modify_dto = DbzSourceConnectorModifyDto::builder()
            .id(source.id)
            .run_status(run_status)
            ._current_user_id(U64(1))
            ._current_ms(U64(now_ms()))
            .build();

        Self::modify::<DatabaseConnection>(modify_dto, None)
            .await
            .map_err(|e| anyhow!("更新上游连接器<{}>运行状态失败: {e}", source.name))?;

        info!(
            "上游连接器<{}>运行状态已更新: {}",
            source.name,
            run_status.value()
        );
        Ok(())
    }
}
