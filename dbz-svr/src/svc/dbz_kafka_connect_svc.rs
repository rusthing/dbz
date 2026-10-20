//! # Kafka Connect 服务
//!
//! Kafka Connect 是连接器实际运行的集群：一台集群可挂多条上游与多条下游连接器
//! （各为 1:N），上游/下游只保存 `kafka_connect_id`，地址、启停与连通状态都收敛
//! 到 `dbz_kafka_connect`。
//!
//! 拆分动机（原先地址挂在上游连接器上）：同一台集群被重复配置 N 遍；下游必须借
//! 上游的地址才能定位自己；集群的连通性与启停无处表达；一台集群宕机按上游条数
//! 刷告警。拆出来后，集群成为可单独启停、单独记状态、每轮只探测一次的运维对象。
//!
//! ## 健康检查
//!
//! 以集群为入口（[`check_all_connectors`](Self::check_all_connectors)）：逐个集群
//! 探测连通性 → 检查名下每条已启用上游连接器 → 检查名下每条已启用下游连接器，
//! 各自回写运行状态、各自告警。

use crate::dic::{ConnectorRunStatus, KafkaConnectStatus, KafkaConnectorStatus};
use crate::dto::{DbzSinkConnectorQueryDto, DbzSourceConnectorQueryDto};
use crate::svc::{DbzSinkConnectorSvc, DbzSourceConnectorSvc};
use crate::utils::{DbzAlarmUtils, KafkaConnectorError, KafkaConnectorUtils, normalize_url};
use crate::vo::{DbzSinkConnectorVo, DbzSourceConnectorVo};
use anyhow::anyhow;
use robotech::db::get_db_conn;
use robotech::macros::svc;
use sea_orm::DatabaseConnection;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tracing::{info, warn};
use wheel_rs::time_utils::now_ms;

/// 刚部署的连接器等待状态就绪的最大重试次数
const DEPLOY_STATUS_RETRIES: usize = 5;

/// 刚部署的连接器等待状态就绪的重试间隔（毫秒）
const DEPLOY_STATUS_INTERVAL_MS: u64 = 300;

#[svc(
    before_add = DbzKafkaConnectSvc::before_add_hook,
    before_modify = DbzKafkaConnectSvc::before_modify_hook,
    before_del_by_id = DbzKafkaConnectSvc::before_del_by_id_hook
)]
pub struct DbzKafkaConnectSvc;

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
/// 上游与下游连接器的检查流程完全一致，差别只在身份（日志里的标签）与地址，
/// 因此抽成同一组入参。
struct KafkaConnectorCheck<'a> {
    /// 日志里标识这个连接器，如 `上游连接器<mysql-src>(id=1)`
    label: &'a str,
    /// Kafka Connect 地址
    base_url: &'a str,
    /// 连接器名称
    connector_name: &'a str,
    /// 连接器配置（JSON）
    connector_config: &'a str,
}

// SvcError 由框架定义且体积较大，而钩子/服务方法的返回类型必须与宏约定一致，
// 无法改成 Box 包裹，这里统一放行该 lint
#[allow(clippy::result_large_err)]
impl DbzKafkaConnectSvc {
    /// # 新增前钩子：新建即禁用时直接落为「已禁用」
    ///
    /// 被禁用的集群不参与健康检查，不在这里写状态就会一直停在「无状态」。
    async fn before_add_hook(
        mut add_dto: DbzKafkaConnectAddDto,
    ) -> Result<DbzKafkaConnectAddDto, SvcError> {
        if add_dto.enabled == Some(false) {
            add_dto.run_status = Some(KafkaConnectStatus::Disabled);
        }
        Ok(add_dto)
    }

    /// # 修改前钩子：改地址、禁用时先清空旧集群上的连接器
    ///
    /// 集群记录上只有**地址**与**开关**两处会影响 Kafka Connect 上的实际部署：
    ///
    /// 1. **改地址**：连接器不会跟着记录搬家，旧地址上的仍在跑；不注销的话，紧接着
    ///    的检查会在新地址再部署一份，两边同时消费同一份变更。
    /// 2. **禁用**：同样先注销再写库；注销失败则整个修改请求失败 —— 宁可禁用不成，
    ///    也不允许「库里已禁用、连接器还在跑」。
    ///
    /// 注销后**不立即在新地址部署**：一个地址变更牵动整台集群下的所有连接器，逐条
    /// 部署中途失败会出现「新地址已建、库里仍指旧地址」，下一轮又在旧地址建一份，
    /// 才是真正的双写。这里统一置「无状态」，交给下一轮检查整批部署。
    ///
    /// ## 返回值
    /// * `Ok(ModifyDto)` - 补好 `run_status` 的修改参数
    /// * `Err(SvcError)` - 查询原记录失败，或注销连接器失败
    async fn before_modify_hook(
        mut modify_dto: DbzKafkaConnectModifyDto,
    ) -> Result<DbzKafkaConnectModifyDto, SvcError> {
        let Some(id) = modify_dto.id else {
            return Ok(modify_dto);
        };
        // 健康检查只回写 run_status，两个判断都用不上，跳过这次查询
        if modify_dto.enabled.is_none() && modify_dto.url.is_none() {
            return Ok(modify_dto);
        }
        let Some(old) = Self::get_by_id::<DatabaseConnection>(id, None).await?.extra else {
            return Ok(modify_dto);
        };

        let old_url = normalize_url(&old.url);
        let new_url = modify_dto
            .url
            .as_deref()
            .map(normalize_url)
            .unwrap_or_else(|| old_url.clone());
        let url_changed = new_url != old_url;
        let disabling = modify_dto.enabled == Some(false) && old.enabled;
        let enabled_changed = modify_dto
            .enabled
            .is_some_and(|enabled| enabled != old.enabled);
        let enabled_after = modify_dto.enabled.unwrap_or(old.enabled);

        // 地址为空说明这台集群上从未部署过连接器，没有可注销的东西
        if !old_url.is_empty() && (url_changed || disabling) {
            info!(
                "Kafka Connect<{}>(id={}): 地址变更或禁用，注销 <{old_url}> 上的全部连接器",
                old.name,
                old.id.value()
            );
            if let Err(e) = Self::delete_all_connectors(old.id, &old_url).await {
                // 本次是「禁用」时失败必须拦下（fail-closed）；已经禁用过的集群，
                // 连接器在禁用那一次就该注销干净了，这时多半是旧地址早就不可达，
                // 拦住只会让错地址永远改不掉，记日志放行。
                if old.enabled {
                    return Err(e);
                }
                warn!(
                    "Kafka Connect<{}>(id={}): 集群已禁用，注销 <{old_url}> 上的连接器失败({e})，\
                     多为旧地址已不可达，继续修改",
                    old.name,
                    old.id.value()
                );
            }
        }

        if disabling {
            // 连接器已从集群上删除，状态改为「已禁用」
            Self::set_connectors_status(old.id, ConnectorRunStatus::Disabled).await?;
        } else if enabled_after && (url_changed || enabled_changed) {
            // 改地址或重新启用后，连接器要在（新）地址上重新部署，
            // 实际状态等下一轮检查确认
            Self::set_connectors_status(old.id, ConnectorRunStatus::None).await?;
        }

        if enabled_changed || url_changed {
            // 已禁用的集群保持「已禁用」：地址改了也不会去探测
            modify_dto.run_status = Some(if enabled_after {
                KafkaConnectStatus::None
            } else {
                KafkaConnectStatus::Disabled
            });
        }

        Ok(modify_dto)
    }

    /// # 删除前钩子：名下还有连接器时拒绝删除
    ///
    /// 数据库侧已经是 `on delete restrict`，这里提前拦一道，把外键错误换成
    /// 可读的提示。连接器是否启用不影响判断：外键约束不看 `enabled`。
    ///
    /// 能走到这里说明该集群下已没有连接器记录，也就没有需要注销的连接器。
    ///
    /// ## 返回值
    /// * `Ok(U64)` - 可以删除，原样返回 ID
    /// * `Err(SvcError)` - 该集群下仍有连接器
    async fn before_del_by_id_hook(id: U64) -> Result<U64, SvcError> {
        let source_count = Self::list_sources_of(id).await?.len();
        let sink_count = Self::list_sinks_of(id).await?.len();
        if source_count > 0 || sink_count > 0 {
            return Err(anyhow!(
                "Kafka Connect(id={})下还有 {source_count} 条上游连接器、{sink_count} 条下游连接器，\
                 请先删除或改挂这些连接器",
                id.value()
            )
            .into());
        }
        Ok(id)
    }

    /// # 取集群记录的地址
    ///
    /// 连接器钩子、运维接口都通过它定位 Kafka Connect。
    ///
    /// ## 参数
    /// * `id` - Kafka Connect ID
    ///
    /// ## 返回值
    /// * `Ok(String)` - 去掉首尾空白与末尾 `/` 的地址
    /// * `Err(anyhow::Error)` - 记录不存在或地址为空
    pub(crate) async fn get_url(id: U64) -> Result<String, anyhow::Error> {
        let one = Self::get_by_id::<DatabaseConnection>(id, None)
            .await
            .map_err(|e| anyhow!("查询 Kafka Connect<{}>失败: {e}", id.value()))?
            .extra
            .ok_or_else(|| anyhow!("Kafka Connect<{}>不存在", id.value()))?;

        let url = normalize_url(&one.url);
        if url.is_empty() {
            return Err(anyhow!("Kafka Connect<{}>未配置地址", one.name));
        }
        Ok(url)
    }

    /// # 定时检查所有 Kafka Connect 与连接器
    ///
    /// 由 jobx 健康检查任务定时触发；单个连接器检查失败不会中断整体遍历，
    /// 基础设施错误（数据库读写失败）才会中断并返回错误。
    ///
    /// 告警收敛：集群不可达时，每条已启用上游连接器各报一条（与拆分前一致）；
    /// 下游连接器只在**它的上游不在这台故障集群上**时才单独告警 —— 上游在同一台
    /// 集群时已经报过，不再为同一根因刷屏。
    ///
    /// ## 返回值
    /// * `Ok(String)` - 本次检查的执行摘要
    /// * `Err` - 基础设施错误（如数据库连接失败）
    pub async fn check_all_connectors() -> anyhow::Result<String> {
        info!("开始定时检查所有 Kafka Connect 与连接器...");

        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;

        let kc_ro = Self::list_by_query_dto::<DatabaseConnection>(
            DbzKafkaConnectQueryDto::builder().build(),
            Some(&db),
        )
        .await
        .map_err(|e| anyhow!("查询 Kafka Connect 列表失败: {e}"))?;
        let kafka_connects = kc_ro.extra.unwrap_or_default();

        // 连接器一次全取回再按集群分组，避免逐个集群查库；已禁用的也要取，
        // 集群被禁用时要把它们的运行状态一并改为「已禁用」。
        let source_ro = DbzSourceConnectorSvc::list_by_query_dto::<DatabaseConnection>(
            DbzSourceConnectorQueryDto::builder().build(),
            Some(&db),
        )
        .await
        .map_err(|e| anyhow!("查询上游连接器列表失败: {e}"))?;
        let sources = source_ro.extra.unwrap_or_default();

        let sink_ro = DbzSinkConnectorSvc::list_by_query_dto::<DatabaseConnection>(
            DbzSinkConnectorQueryDto::builder().build(),
            Some(&db),
        )
        .await
        .map_err(|e| anyhow!("查询下游连接器列表失败: {e}"))?;
        let sinks = sink_ro.extra.unwrap_or_default();

        let mut sources_by_kc: HashMap<u64, Vec<DbzSourceConnectorVo>> = HashMap::new();
        let mut sources_by_id: HashMap<u64, DbzSourceConnectorVo> = HashMap::new();
        for source in sources {
            sources_by_kc
                .entry(source.kafka_connect_id.value())
                .or_default()
                .push(source.clone());
            sources_by_id.insert(source.id.value(), source);
        }
        let mut sinks_by_kc: HashMap<u64, Vec<DbzSinkConnectorVo>> = HashMap::new();
        for sink in sinks {
            sinks_by_kc
                .entry(sink.kafka_connect_id.value())
                .or_default()
                .push(sink);
        }

        info!(
            "共找到 {} 台 Kafka Connect、{} 条上游连接器、{} 条下游连接器，开始检查",
            kafka_connects.len(),
            sources_by_id.len(),
            sinks_by_kc.values().map(Vec::len).sum::<usize>()
        );

        let mut errors: Vec<String> = Vec::new();
        for kc in &kafka_connects {
            let kc_sources = sources_by_kc.remove(&kc.id.value()).unwrap_or_default();
            let kc_sinks = sinks_by_kc.remove(&kc.id.value()).unwrap_or_default();
            match Self::check_one_kafka_connect(kc, &kc_sources, &kc_sinks, &sources_by_id).await {
                Ok(mut kc_errors) => errors.append(&mut kc_errors),
                Err(e) => {
                    warn!(
                        "检查 Kafka Connect<{}>(id={})失败: {e}",
                        kc.name,
                        kc.id.value()
                    );
                    errors.push(format!(
                        "Kafka Connect<{}>(id={}): {e}",
                        kc.name,
                        kc.id.value()
                    ));
                }
            }
        }

        let exec_detail = if errors.is_empty() {
            let msg = "所有 Kafka Connect 与连接器状态检查完成，一切正常";
            info!("{}", msg);
            msg.to_string()
        } else {
            let msg = format!("连接器状态检查完成，{} 个对象存在异常", errors.len());
            warn!("{}", msg);
            msg
        };

        Ok(exec_detail)
    }

    /// # 检查一台 Kafka Connect 及其名下的连接器
    ///
    /// ## 参数
    /// * `kc` - Kafka Connect 记录
    /// * `sources` - 挂在这台集群上的上游连接器（含已禁用）
    /// * `sinks` - 挂在这台集群上的下游连接器（含已禁用）
    /// * `sources_by_id` - 全部上游连接器，用于下游告警时取上游名称
    ///
    /// ## 返回值
    /// * `Ok(Vec<String>)` - 异常对象的详情列表；全部正常时为空
    /// * `Err(anyhow::Error)` - 基础设施错误（数据库读写失败）
    async fn check_one_kafka_connect(
        kc: &DbzKafkaConnectVo,
        sources: &[DbzSourceConnectorVo],
        sinks: &[DbzSinkConnectorVo],
        sources_by_id: &HashMap<u64, DbzSourceConnectorVo>,
    ) -> Result<Vec<String>, anyhow::Error> {
        let kc_label = format!("Kafka Connect<{}>(id={})", kc.name, kc.id.value());
        let url = normalize_url(&kc.url);
        let mut errors: Vec<String> = Vec::new();

        let enabled_sources: Vec<&DbzSourceConnectorVo> =
            sources.iter().filter(|source| source.enabled).collect();
        let enabled_sinks: Vec<&DbzSinkConnectorVo> =
            sinks.iter().filter(|sink| sink.enabled).collect();

        // 集群被禁用时，连接器已在禁用那一次由钩子注销，这里不探测也不注销，只把
        // 集群与名下连接器的状态对齐为「已禁用」，否则会停留在上次检查的结论上。
        if !kc.enabled {
            // 幂等：状态已一致时 update_status 不写库
            Self::update_status(kc, KafkaConnectStatus::Disabled).await?;
            for source in sources {
                DbzSourceConnectorSvc::update_run_status(source, ConnectorRunStatus::Disabled)
                    .await?;
            }
            for sink in sinks {
                DbzSinkConnectorSvc::update_run_status(sink, ConnectorRunStatus::Disabled).await?;
            }
            return Ok(errors);
        }

        // 集群自身的连通性每轮都探测一次：即使暂时没有连接器，记录上的状态也应当
        // 反映「这台集群现在通不通」，否则新建的集群会一直停在「无状态」。
        let reachable = if url.is_empty() {
            warn!("{kc_label}: 未配置地址，按连接失败处理");
            false
        } else {
            match KafkaConnectorUtils::check_connect(&url).await {
                Ok(()) => true,
                Err(e) => {
                    warn!("{kc_label}: 连接失败: {e}");
                    false
                }
            }
        };

        if !reachable {
            Self::update_status(kc, KafkaConnectStatus::ConnectFailed).await?;
            // 集群自身也算一个异常对象：名下没有连接器时，不记这一条，整轮会汇总成
            // 「一切正常」，可运维其实连不上这台集群。
            errors.push(format!(
                "{kc_label}: {}",
                KafkaConnectStatus::ConnectFailed.value()
            ));
            let status = ConnectorRunStatus::KafkaConnectFailed;
            // 已经在上面报过告警的上游，用来判断下游是否还需要单独告警
            let alarmed_sources: HashSet<u64> = enabled_sources
                .iter()
                .map(|source| source.id.value())
                .collect();
            for source in &enabled_sources {
                DbzSourceConnectorSvc::update_run_status(source, status).await?;
                Self::send_source_alarm(source, kc, status).await;
                errors.push(format!(
                    "上游连接器<{}>(id={}): {}",
                    source.name,
                    source.id.value(),
                    status.value()
                ));
            }
            for sink in &enabled_sinks {
                DbzSinkConnectorSvc::update_run_status(sink, status).await?;
                // 上游也在这台故障集群上、且已经报过告警时，不再为同一根因重复上报；
                // 上游不在故障集群上（或上游已禁用、没有告警）时，下游必须自己报。
                if !alarmed_sources.contains(&sink.source_connector_id.value()) {
                    Self::send_sink_alarm(sink, sources_by_id, kc, status).await;
                }
                errors.push(format!(
                    "下游连接器<{}>(id={}): {}",
                    sink.name,
                    sink.id.value(),
                    status.value()
                ));
            }
            return Ok(errors);
        }

        Self::update_status(kc, KafkaConnectStatus::Normal).await?;

        // 上游连接器
        for source in &enabled_sources {
            let label = format!("上游连接器<{}>(id={})", source.name, source.id.value());
            let check = KafkaConnectorCheck {
                label: &label,
                base_url: &url,
                connector_name: &source.connector_name,
                connector_config: &source.connector_config,
            };
            match Self::ensure_and_check_connector(&check).await {
                Ok(()) => {
                    DbzSourceConnectorSvc::update_run_status(source, ConnectorRunStatus::Running)
                        .await?
                }
                Err((run_status, action)) => {
                    DbzSourceConnectorSvc::update_run_status(source, run_status).await?;
                    Self::send_source_alarm(source, kc, run_status).await;
                    // 告警先发，再修复：自动修复不替代报警
                    Self::apply_repair(&url, &source.connector_name, action).await;
                    errors.push(format!("{label}: {}", run_status.value()));
                }
            }
        }

        // 下游连接器
        for sink in &enabled_sinks {
            let source_name = sources_by_id
                .get(&sink.source_connector_id.value())
                .map(|source| source.name.as_str())
                .unwrap_or("未知");
            let label = format!(
                "下游连接器<{}>(id={}, 上游<{}>)",
                sink.name,
                sink.id.value(),
                source_name
            );
            let check = KafkaConnectorCheck {
                label: &label,
                base_url: &url,
                connector_name: &sink.connector_name,
                connector_config: &sink.connector_config,
            };
            match Self::ensure_and_check_connector(&check).await {
                Ok(()) => {
                    DbzSinkConnectorSvc::update_run_status(sink, ConnectorRunStatus::Running)
                        .await?
                }
                Err((run_status, action)) => {
                    DbzSinkConnectorSvc::update_run_status(sink, run_status).await?;
                    Self::send_sink_alarm(sink, sources_by_id, kc, run_status).await;
                    // 告警先发，再修复：自动修复不替代报警
                    Self::apply_repair(&url, &sink.connector_name, action).await;
                    errors.push(format!("{label}: {}", run_status.value()));
                }
            }
        }

        Ok(errors)
    }

    /// # 上报上游连接器告警
    ///
    /// 告警是旁路动作：上报失败只记日志，不能中断检查流程（否则一台 alarm-svr
    /// 故障会让整轮检查提前结束、自动修复也不再执行）。
    async fn send_source_alarm(
        source: &DbzSourceConnectorVo,
        kc: &DbzKafkaConnectVo,
        run_status: ConnectorRunStatus,
    ) {
        if let Err(e) = DbzAlarmUtils::send_source_connector_alarm(source, kc, run_status).await {
            warn!("上游连接器<{}>告警上报失败: {e}", source.name);
        }
    }

    /// # 上报下游连接器告警
    ///
    /// 与 [`send_source_alarm`](Self::send_source_alarm) 一样，失败只记日志。
    async fn send_sink_alarm(
        sink: &DbzSinkConnectorVo,
        sources_by_id: &HashMap<u64, DbzSourceConnectorVo>,
        kc: &DbzKafkaConnectVo,
        run_status: ConnectorRunStatus,
    ) {
        let source_name = sources_by_id
            .get(&sink.source_connector_id.value())
            .map(|source| source.name.as_str())
            .unwrap_or("未知");
        if let Err(e) =
            DbzAlarmUtils::send_sink_connector_alarm(sink, source_name, kc, run_status).await
        {
            warn!("下游连接器<{}>告警上报失败: {e}", sink.name);
        }
    }

    /// # 确保连接器已注册并检查其状态
    ///
    /// 先检查连接器是否存在，不存在则部署；然后查询连接器与任务的状态。
    /// 若检查失败、部署失败或状态异常，返回对应的错误状态和修复动作。
    ///
    /// ## 返回值
    /// * `Ok(())` - 连接器及其任务都正常运行
    /// * `Err((ConnectorRunStatus, RepairAction))` - 连接器异常，返回对应的运行状态与修复动作
    async fn ensure_and_check_connector(
        check: &KafkaConnectorCheck<'_>,
    ) -> Result<(), (ConnectorRunStatus, RepairAction)> {
        let KafkaConnectorCheck {
            label,
            base_url,
            connector_name,
            connector_config,
        } = *check;

        // 检查连接器是否存在，不存在则部署
        let exists = KafkaConnectorUtils::connector_exists(base_url, connector_name)
            .await
            .map_err(|e| {
                warn!("{label}: 检查连接器<{connector_name}>是否存在失败: {e}");
                (ConnectorRunStatus::CheckFailed, RepairAction::None)
            })?;

        if !exists {
            info!("{label}: 连接器<{connector_name}>不存在，开始部署...");
            KafkaConnectorUtils::deploy_connector(base_url, connector_name, connector_config)
                .await
                .map_err(|e| {
                    warn!("{label}: 部署连接器<{connector_name}>失败: {e}");
                    (ConnectorRunStatus::RegisterFailed, RepairAction::None)
                })?;
            info!("{label}: 连接器<{connector_name}>部署成功");
        }

        // 查询连接器状态（含任务状态）。刚部署的连接器会短暂处于 UNASSIGNED，
        // 这时判异常会误告警并触发无意义的重启，先等它过渡到实际状态。
        let (status, failed_task_ids) =
            Self::fetch_status(base_url, connector_name, label, !exists)
                .await
                .map_err(|e| {
                    warn!("{label}: 查询连接器<{connector_name}>状态失败: {e}");
                    (ConnectorRunStatus::CheckStatusFailed, RepairAction::None)
                })?;

        if status != KafkaConnectorStatus::Running {
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
            if status == KafkaConnectorStatus::Unassigned {
                warn!("{label}: 连接器<{connector_name}>返回未知状态: {status}");
                return Err((
                    ConnectorRunStatus::CheckError,
                    RepairAction::RestartConnector,
                ));
            }
            warn!("{label}: 连接器<{connector_name}>状态异常: {status}");
            // 只有 failed 有确定的修复动作；paused 多为人工维护、destroyed 只能重建，都不动
            let action = if status == KafkaConnectorStatus::Failed {
                RepairAction::RestartConnector
            } else {
                RepairAction::None
            };
            return Err((ConnectorRunStatus::CheckStatusError, action));
        }

        Ok(())
    }

    /// # 查询连接器状态，刚部署时等待其脱离 UNASSIGNED
    ///
    /// 部署接口返回成功只代表配置已被接受，连接器与任务需要一点时间才会被分配；
    /// 这段时间里状态是 `UNASSIGNED`。直接按 `UNASSIGNED` 判定会产生误告警，
    /// 因此刚部署时多查几次，超过重试次数仍为 `UNASSIGNED` 才交给上层判定。
    ///
    /// ## 参数
    /// * `base_url` - Kafka Connect 地址
    /// * `connector_name` - 连接器名称
    /// * `label` - 日志标签
    /// * `just_deployed` - 本次是否刚刚部署过该连接器
    async fn fetch_status(
        base_url: &str,
        connector_name: &str,
        label: &str,
        just_deployed: bool,
    ) -> Result<(KafkaConnectorStatus, Vec<u32>), KafkaConnectorError> {
        let mut result =
            KafkaConnectorUtils::fetch_connector_status(base_url, connector_name).await?;
        if !just_deployed {
            return Ok(result);
        }

        for _ in 0..DEPLOY_STATUS_RETRIES {
            if result.0 != KafkaConnectorStatus::Unassigned {
                break;
            }
            info!("{label}: 连接器<{connector_name}>刚部署，等待状态就绪...");
            tokio::time::sleep(Duration::from_millis(DEPLOY_STATUS_INTERVAL_MS)).await;
            result = KafkaConnectorUtils::fetch_connector_status(base_url, connector_name).await?;
        }
        Ok(result)
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
                    KafkaConnectorUtils::restart_connector(base_url, connector_name, true, false)
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
                    if let Err(e) = KafkaConnectorUtils::restart_connector_task(
                        base_url,
                        connector_name,
                        task_id,
                    )
                    .await
                    {
                        warn!("连接器<{connector_name}>的任务<{task_id}>自动重启失败: {e}");
                    }
                }
            }
        }
    }

    /// # 更新集群记录的状态
    ///
    /// **状态没变就不写库**：检查每轮都在跑，状态常年是「正常」，每轮写一次只会
    /// 让 `update_ms` 失去意义。不写之后 `update_ms` 表示的是「进入当前状态的时间」。
    async fn update_status(
        kc: &DbzKafkaConnectVo,
        run_status: KafkaConnectStatus,
    ) -> Result<(), anyhow::Error> {
        if kc.run_status == run_status {
            return Ok(());
        }

        let modify_dto = DbzKafkaConnectModifyDto::builder()
            .id(kc.id)
            .run_status(run_status)
            ._current_user_id(U64(1))
            ._current_ms(U64(now_ms()))
            .build();

        Self::modify::<DatabaseConnection>(modify_dto, None)
            .await
            .map_err(|e| anyhow!("更新 Kafka Connect<{}>状态失败: {e}", kc.name))?;

        info!(
            "Kafka Connect<{}>状态已更新: {}",
            kc.name,
            run_status.value()
        );
        Ok(())
    }

    /// # 把某台集群名下的连接器状态统一改成指定值
    ///
    /// 用于集群被禁用（全部标为「已禁用」）与改地址、重新启用（全部标为「无状态」）。
    ///
    /// 置「无状态」是「等下一轮检查重新部署」的意思，只对**已启用**的记录有意义：
    /// 已禁用的记录本该停在「已禁用」，不能被顺手改成「无状态」—— 它们不进健康
    /// 检查，改过去就再也没有机会被改回来。
    async fn set_connectors_status(
        kafka_connect_id: U64,
        run_status: ConnectorRunStatus,
    ) -> Result<(), SvcError> {
        let only_enabled = run_status == ConnectorRunStatus::None;
        for source in Self::list_sources_of(kafka_connect_id).await? {
            if only_enabled && !source.enabled {
                continue;
            }
            DbzSourceConnectorSvc::update_run_status(&source, run_status).await?;
        }
        for sink in Self::list_sinks_of(kafka_connect_id).await? {
            if only_enabled && !sink.enabled {
                continue;
            }
            DbzSinkConnectorSvc::update_run_status(&sink, run_status).await?;
        }
        Ok(())
    }

    /// # 注销某台集群上的全部连接器
    ///
    /// 记录删不掉、只是地址变了或集群被禁用时使用：连接器必须先从 Kafka Connect
    /// 上摘掉，否则会变成没人管理、也不会再告警的「幽灵连接器」。
    ///
    /// ## 参数
    /// * `kafka_connect_id` - Kafka Connect ID
    /// * `url` - 该集群**原**地址
    async fn delete_all_connectors(kafka_connect_id: U64, url: &str) -> Result<(), SvcError> {
        for source in Self::list_sources_of(kafka_connect_id).await? {
            info!(
                "上游连接器<{}>(id={}): 从 <{url}> 注销连接器<{}>",
                source.name,
                source.id.value(),
                source.connector_name
            );
            KafkaConnectorUtils::delete_connector(url, &source.connector_name).await?;
        }
        for sink in Self::list_sinks_of(kafka_connect_id).await? {
            info!(
                "下游连接器<{}>(id={}): 从 <{url}> 注销连接器<{}>",
                sink.name,
                sink.id.value(),
                sink.connector_name
            );
            KafkaConnectorUtils::delete_connector(url, &sink.connector_name).await?;
        }
        Ok(())
    }

    /// # 查询某台集群名下的全部上游连接器
    async fn list_sources_of(kafka_connect_id: U64) -> Result<Vec<DbzSourceConnectorVo>, SvcError> {
        let query_dto = DbzSourceConnectorQueryDto::builder()
            .kafka_connect_id(kafka_connect_id)
            .build();
        Ok(
            DbzSourceConnectorSvc::list_by_query_dto::<DatabaseConnection>(query_dto, None)
                .await?
                .extra
                .unwrap_or_default(),
        )
    }

    /// # 查询某台集群名下的全部下游连接器
    async fn list_sinks_of(kafka_connect_id: U64) -> Result<Vec<DbzSinkConnectorVo>, SvcError> {
        let query_dto = DbzSinkConnectorQueryDto::builder()
            .kafka_connect_id(kafka_connect_id)
            .build();
        Ok(
            DbzSinkConnectorSvc::list_by_query_dto::<DatabaseConnection>(query_dto, None)
                .await?
                .extra
                .unwrap_or_default(),
        )
    }
}
