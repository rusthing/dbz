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

use crate::dao::{DbzSinkConnectorDao, DbzSourceConnectorDao};
use crate::dic::{ConnectorRunStatus, KafkaConnectStatus, KafkaConnectorStatus};
use crate::dto::{
    DbzConnectorDetectDto, DbzKafkaConnectDetectDto, DbzKafkaConnectHealthDto,
    DbzSinkConnectorQueryDto, DbzSourceConnectorQueryDto,
};
use crate::mo::dbz_sink_connector::ActiveModel as SinkActiveModel;
use crate::mo::dbz_source_connector::ActiveModel as SourceActiveModel;
use crate::svc::{DbzKafkaBrokerSvc, DbzSinkConnectorSvc, DbzSourceConnectorSvc};
use crate::utils::{
    ConnectorHealth, DbzAlarmUtils, DbzSignalUtils, KafkaConnectorError, KafkaConnectorUtils,
    RUN_MSG_MAX, normalize_url, one_line,
};
use crate::vo::{
    DbzConnectorCompareDetail, DbzConnectorCompareRow, DbzConnectorDetectItemVo,
    DbzConnectorDetectVo, DbzKafkaConnectDetectVo, DbzKafkaConnectHealthVo, DbzSinkConnectorVo,
    DbzSourceConnectorVo,
};
use anyhow::anyhow;
use robotech::db::get_db_conn;
use robotech::macros::svc;
use sea_orm::{DatabaseConnection, Set};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
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

/// # 一次检查发现的故障
///
/// 三样东西一起回给调用方：要回写的状态、要执行的修复、以及**给人看的原因**。
/// 原因来自 Kafka Connect 的响应（被拒的配置、异常任务的 trace 首行），是排查时
/// 唯一有用的信息；只回状态的话它就只剩日志里的一行了。
struct ConnectorFault {
    /// 要回写的运行状态
    status: ConnectorRunStatus,
    /// 自动修复动作
    action: RepairAction,
    /// 失败原因（写进 `run_msg`，页面上鼠标悬停可见）
    reason: Option<String>,
}

impl ConnectorFault {
    /// 只带状态与修复动作，没有额外原因（原因已经由状态本身说清时用）
    fn new(status: ConnectorRunStatus, action: RepairAction) -> Self {
        Self {
            status,
            action,
            reason: None,
        }
    }

    /// 带上一句原因（会压成一行并截断）
    fn with_reason(mut self, reason: impl AsRef<str>) -> Self {
        self.reason = Some(one_line(reason.as_ref(), RUN_MSG_MAX));
        self
    }
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
    /// `kafka_broker_id` 由 DTO 与库列双重保证必填（没有 broker 地址就发不了 Kafka
    /// 信号、查不了 topic，建一条查不到后端的记录没有意义），这里不再重复校验。
    ///
    /// 被禁用的集群不参与健康检查，不在这里写状态就会一直停在「无状态」。
    async fn before_add_hook<C: ConnectionTrait>(
        mut add_dto: DbzKafkaConnectAddDto,
        _db: &C,
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
    async fn before_modify_hook<C: ConnectionTrait>(
        mut modify_dto: DbzKafkaConnectModifyDto,
        _db: &C,
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
            Self::set_connectors_status(old.id, ConnectorRunStatus::Unknown).await?;
        }

        if enabled_changed || url_changed {
            // 已禁用的集群保持「已禁用」：地址改了也不会去探测
            modify_dto.run_status = Some(if enabled_after {
                KafkaConnectStatus::None
            } else {
                KafkaConnectStatus::Disabled
            });
            // 改地址或重新启用后，上一轮对着旧地址得出的失败原因已经不适用
            modify_dto.run_msg = Some(None);
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
    async fn before_del_by_id_hook<C: ConnectionTrait>(id: U64, _db: &C) -> Result<U64, SvcError> {
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

    /// # 探测 Kafka Connect 自身健康状态
    ///
    /// 只读接口，**不回写** `run_status`：回写由定时健康检查负责，GET 不应产生副作用。
    ///
    /// 可达性以 `GET /connectors` 为准，版本、commit、Kafka cluster id 来自根路径；
    /// 根路径取不到时只留空，不影响可达性结论。
    ///
    /// 与 broker 的 [`check`](DbzKafkaBrokerSvc::check) 同一套口径：连不上属于业务
    /// 结论，返回 200 + `reachable=false` 与失败原因；只有记录不存在或地址为空才报错。
    ///
    /// ## 参数
    /// * `dto` - Kafka Connect ID
    ///
    /// ## 返回值
    /// * `Ok(Ro<DbzKafkaConnectHealthVo>)` - 可达性、版本、worker 数、耗时等
    /// * `Err(anyhow::Error)` - 记录不存在或未配置地址
    #[log_call]
    pub async fn health(
        dto: DbzKafkaConnectHealthDto,
    ) -> Result<Ro<DbzKafkaConnectHealthVo>, anyhow::Error> {
        let one = Self::get_by_id::<DatabaseConnection>(dto.kafka_connect_id, None)
            .await?
            .extra
            .ok_or_else(|| anyhow!("Kafka Connect<{}>不存在", dto.kafka_connect_id.value()))?;

        if !one.enabled {
            let result = DbzKafkaConnectHealthVo {
                reachable: false,
                elapsed_ms: 0,
                message: Some("记录已禁用，未发起健康检查".to_string()),
                ..Default::default()
            };
            return Ok(Ro::success("记录已禁用，未发起健康检查".to_string()).extra(Some(result)));
        }

        let url = normalize_url(&one.url);
        if url.is_empty() {
            return Err(anyhow!("Kafka Connect<{}>未配置地址", one.name));
        }

        let started = Instant::now();
        let result = match KafkaConnectorUtils::probe_connect(&url).await {
            Ok(probe) => {
                info!(
                    "Kafka Connect<{}>(id={})可达，版本 {:?}，{} 个连接器，{} 个 worker",
                    one.name,
                    one.id.value(),
                    probe.version,
                    probe.connector_count,
                    probe.worker_ids.len()
                );
                DbzKafkaConnectHealthVo {
                    reachable: true,
                    version: probe.version,
                    commit: probe.commit,
                    kafka_cluster_id: probe.kafka_cluster_id,
                    connector_count: probe.connector_count,
                    worker_count: probe.worker_ids.len() as u32,
                    worker_ids: probe.worker_ids,
                    elapsed_ms: started.elapsed().as_millis() as u64,
                    message: None,
                }
            }
            Err(e) => {
                warn!(
                    "Kafka Connect<{}>(id={})健康检查失败: {e}",
                    one.name,
                    one.id.value()
                );
                DbzKafkaConnectHealthVo {
                    reachable: false,
                    elapsed_ms: started.elapsed().as_millis() as u64,
                    message: Some(format!("{e:#}")),
                    ..Default::default()
                }
            }
        };

        let msg = if result.reachable {
            format!(
                "连接正常，{} 个连接器，{} 个 worker",
                result.connector_count, result.worker_count
            )
        } else {
            "连接失败".to_string()
        };
        Ok(Ro::success(msg).extra(Some(result)))
    }

    /// # 检测并同步 Kafka Connect 上的连接器
    ///
    /// 以 Connect 上的实际连接器为准，把差异落到库内纳管记录：
    ///
    /// * 远端有、库内没有 → 自动补录（下游连接器按 topic 前缀尽量匹配上游）；
    /// * 配置与库内不一致 → 标记 `inconsistent=true`；
    /// * 远端已不存在 → 置为禁用并标记 `inconsistent=true`。
    ///
    /// 只比对 `connectorConfig` 内容与远端存在性，**不看运行状态**：运行状态会
    /// 随任务重启、rebalance 抖动，用它判断会天天误报。配置比较只覆盖库内配置里
    /// 出现过的键，Connect 自动补的默认值不参与比较。
    ///
    /// ## 参数
    /// * `dto` - Kafka Connect ID
    ///
    /// ## 返回值
    /// * `Ok(Ro<DbzKafkaConnectDetectVo>)` - 补录/标记/禁用/跳过的数量与明细
    /// * `Err(anyhow::Error)` - 记录不存在、集群已禁用/未配置地址或远端不可达
    #[log_call]
    pub async fn detect(
        dto: DbzKafkaConnectDetectDto,
    ) -> Result<Ro<DbzKafkaConnectDetectVo>, anyhow::Error> {
        let connect = Self::get_by_id::<DatabaseConnection>(dto.kafka_connect_id, None)
            .await?
            .extra
            .ok_or_else(|| anyhow!("Kafka Connect<{}>不存在", dto.kafka_connect_id.value()))?;

        if !connect.enabled {
            return Err(anyhow!("Kafka Connect<{}>已禁用，未发起检测", connect.name));
        }
        let url = normalize_url(&connect.url);
        if url.is_empty() {
            return Err(anyhow!("Kafka Connect<{}>未配置地址", connect.name));
        }

        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;
        let remote = KafkaConnectorUtils::list_connectors(&url)
            .await
            .map_err(|e| anyhow!("查询 Kafka Connect<{}>连接器列表失败: {e}", connect.name))?;
        let sources = Self::list_sources_of(connect.id).await?;
        let sinks = Self::list_sinks_of(connect.id).await?;

        let mut out = DbzKafkaConnectDetectVo::default();
        let mut remote_names: HashSet<String> = HashSet::new();

        for item in &remote {
            remote_names.insert(item.name.clone());
            let connector_type = item.connector_type.to_ascii_lowercase();

            // 配置读不到时不整次失败：已有记录跳过比对，新连接器也无法补录。
            let remote_config =
                match KafkaConnectorUtils::fetch_connector_config(&url, &item.name).await {
                    Ok(value) => Some(value),
                    Err(e) => {
                        warn!("检测连接器<{}>：读取远端配置失败: {e}", item.name);
                        None
                    }
                };

            if let Some(local) = sources.iter().find(|s| s.connector_name == item.name) {
                let Some(remote_config) = remote_config else {
                    push_detect_item(
                        &mut out,
                        &item.name,
                        "source",
                        "skipped",
                        Some("读取远端配置失败，未比对".to_string()),
                    );
                    continue;
                };
                let mismatch = config_mismatch(&local.connector_config, &remote_config);
                let detail_text = compare_detail_text(
                    &local.connector_config,
                    &remote_config,
                    mismatch,
                    now_ms(),
                    &local.name,
                )?;
                let flag_changed = mismatch != local.inconsistent;
                let snapshot_changed =
                    local.inconsistent_detail.as_deref() != detail_text.as_deref();
                if !flag_changed && !snapshot_changed {
                    push_detect_item(&mut out, &item.name, "source", "unchanged", None);
                    continue;
                }
                let mut active = SourceActiveModel {
                    id: Set(local.id.value() as i64),
                    ..Default::default()
                };
                if flag_changed {
                    active.inconsistent = Set(mismatch);
                }
                active.inconsistent_detail = Set(detail_text);
                active.updater_id = Set(0);
                DbzSourceConnectorDao::update(active, db.as_ref())
                    .await
                    .map_err(|e| anyhow!("回写上游连接器<{}>检测标记失败: {e}", local.name))?;
                push_detect_item(
                    &mut out,
                    &item.name,
                    "source",
                    if flag_changed {
                        if mismatch {
                            "marked"
                        } else {
                            "cleared"
                        }
                    } else {
                        "unchanged"
                    },
                    None,
                );
                continue;
            }

            if let Some(local) = sinks.iter().find(|s| s.connector_name == item.name) {
                let Some(remote_config) = remote_config else {
                    push_detect_item(
                        &mut out,
                        &item.name,
                        "sink",
                        "skipped",
                        Some("读取远端配置失败，未比对".to_string()),
                    );
                    continue;
                };
                let mismatch = config_mismatch(&local.connector_config, &remote_config);
                let detail_text = compare_detail_text(
                    &local.connector_config,
                    &remote_config,
                    mismatch,
                    now_ms(),
                    &local.name,
                )?;
                let flag_changed = mismatch != local.inconsistent;
                let snapshot_changed =
                    local.inconsistent_detail.as_deref() != detail_text.as_deref();
                if !flag_changed && !snapshot_changed {
                    push_detect_item(&mut out, &item.name, "sink", "unchanged", None);
                    continue;
                }
                let mut active = SinkActiveModel {
                    id: Set(local.id.value() as i64),
                    ..Default::default()
                };
                if flag_changed {
                    active.inconsistent = Set(mismatch);
                }
                active.inconsistent_detail = Set(detail_text);
                active.updater_id = Set(0);
                DbzSinkConnectorDao::update(active, db.as_ref())
                    .await
                    .map_err(|e| anyhow!("回写下游连接器<{}>检测标记失败: {e}", local.name))?;
                push_detect_item(
                    &mut out,
                    &item.name,
                    "sink",
                    if flag_changed {
                        if mismatch {
                            "marked"
                        } else {
                            "cleared"
                        }
                    } else {
                        "unchanged"
                    },
                    None,
                );
                continue;
            }

            // 远端有、库内没有：按远端类型补录。
            let Some(remote_config) = remote_config else {
                push_detect_item(
                    &mut out,
                    &item.name,
                    "unknown",
                    "skipped",
                    Some("读取远端配置失败，无法补录".to_string()),
                );
                continue;
            };
            let config_text = serde_json::to_string(&remote_config)
                .map_err(|e| anyhow!("序列化连接器<{}>配置失败: {e}", item.name))?;

            match connector_type.as_str() {
                "source" => {
                    let active = SourceActiveModel {
                        kafka_connect_id: Set(connect.id.value() as i64),
                        name: Set(item.name.clone()),
                        connector_name: Set(item.name.clone()),
                        connector_config: Set(config_text),
                        run_status: Set(ConnectorRunStatus::Unknown.value()),
                        run_msg: Set(None),
                        remark: Set(Some("检测自动补录".to_string())),
                        enabled: Set(true),
                        creater_id: Set(0),
                        ..Default::default()
                    };
                    match DbzSourceConnectorDao::insert(active, db.as_ref()).await {
                        Ok(model) => {
                            info!(
                                "检测补录上游连接器<{}>(id={})到 Kafka Connect<{}>",
                                model.name,
                                model.id,
                                connect.name
                            );
                            push_detect_item(&mut out, &item.name, "source", "added", None);
                        }
                        Err(e) => push_detect_item(
                            &mut out,
                            &item.name,
                            "source",
                            "skipped",
                            Some(format!("补录失败（名称可能已被占用）: {e}")),
                        ),
                    }
                }
                "sink" => {
                    let Some(source_id) = resolve_upstream(&remote_config, &sources) else {
                        push_detect_item(
                            &mut out,
                            &item.name,
                            "sink",
                            "skipped",
                            Some("无法确定上游连接器，未自动补录".to_string()),
                        );
                        continue;
                    };
                    let active = SinkActiveModel {
                        kafka_connect_id: Set(connect.id.value() as i64),
                        source_connector_id: Set(source_id),
                        name: Set(item.name.clone()),
                        connector_name: Set(item.name.clone()),
                        connector_config: Set(config_text),
                        run_status: Set(ConnectorRunStatus::Unknown.value()),
                        run_msg: Set(None),
                        remark: Set(Some("检测自动补录".to_string())),
                        enabled: Set(true),
                        creater_id: Set(0),
                        ..Default::default()
                    };
                    match DbzSinkConnectorDao::insert(active, db.as_ref()).await {
                        Ok(model) => {
                            info!(
                                "检测补录下游连接器<{}>(id={})到 Kafka Connect<{}>",
                                model.name,
                                model.id,
                                connect.name
                            );
                            push_detect_item(&mut out, &item.name, "sink", "added", None);
                        }
                        Err(e) => push_detect_item(
                            &mut out,
                            &item.name,
                            "sink",
                            "skipped",
                            Some(format!("补录失败（名称可能已被占用）: {e}")),
                        ),
                    }
                }
                _ => push_detect_item(
                    &mut out,
                    &item.name,
                    "unknown",
                    "skipped",
                    Some("Connect 未返回连接器类型，无法自动补录".to_string()),
                ),
            }
        }

        // 库内有、远端没有：置为禁用并标记；不删除记录，保留配置便于人工核对。
        for source in sources
            .iter()
            .filter(|s| !remote_names.contains(&s.connector_name))
        {
            let flag_changed = source.enabled || !source.inconsistent;
            // 远端不存在时没有逐项配置可比，快照只记录「已不存在」供前端说明
            let detail_text = missing_detail_text(now_ms(), &source.name)?;
            let snapshot_changed =
                source.inconsistent_detail.as_deref() != detail_text.as_deref();
            if flag_changed || snapshot_changed {
                let mut active = SourceActiveModel {
                    id: Set(source.id.value() as i64),
                    ..Default::default()
                };
                active.enabled = Set(false);
                active.inconsistent = Set(true);
                active.run_status = Set(ConnectorRunStatus::Disabled.value());
                active.run_msg = Set(Some("检测：Connect 上已不存在该连接器".to_string()));
                active.inconsistent_detail = Set(detail_text);
                active.updater_id = Set(0);
                DbzSourceConnectorDao::update(active, db.as_ref())
                    .await
                    .map_err(|e| anyhow!("禁用上游连接器<{}>失败: {e}", source.name))?;
            }
            push_detect_item(
                &mut out,
                &source.connector_name,
                "source",
                if flag_changed { "disabled" } else { "unchanged" },
                None,
            );
        }
        for sink in sinks
            .iter()
            .filter(|s| !remote_names.contains(&s.connector_name))
        {
            let flag_changed = sink.enabled || !sink.inconsistent;
            let detail_text = missing_detail_text(now_ms(), &sink.name)?;
            let snapshot_changed = sink.inconsistent_detail.as_deref() != detail_text.as_deref();
            if flag_changed || snapshot_changed {
                let mut active = SinkActiveModel {
                    id: Set(sink.id.value() as i64),
                    ..Default::default()
                };
                active.enabled = Set(false);
                active.inconsistent = Set(true);
                active.run_status = Set(ConnectorRunStatus::Disabled.value());
                active.run_msg = Set(Some("检测：Connect 上已不存在该连接器".to_string()));
                active.inconsistent_detail = Set(detail_text);
                active.updater_id = Set(0);
                DbzSinkConnectorDao::update(active, db.as_ref())
                    .await
                    .map_err(|e| anyhow!("禁用下游连接器<{}>失败: {e}", sink.name))?;
            }
            push_detect_item(
                &mut out,
                &sink.connector_name,
                "sink",
                if flag_changed { "disabled" } else { "unchanged" },
                None,
            );
        }

        info!(
            "Kafka Connect<{}>检测完成：补录 {}，标记 {}，禁用 {}，恢复 {}，无变化 {}，跳过 {}",
            connect.name, out.added, out.marked, out.disabled, out.cleared, out.unchanged, out.skipped
        );
        Ok(Ro::success(format!(
            "检测完成：补录 {}，标记不一致 {}，置禁用 {}，恢复一致 {}，跳过 {}",
            out.added, out.marked, out.disabled, out.cleared, out.skipped
        ))
        .extra(Some(out)))
    }

    /// # 检测单条上游 / 下游连接器的配置一致性
    ///
    /// 控制面板里连接器卡片上的「检测」：与 {@link detect} 用同一套判定与同一份
    /// 脱敏快照，但作用范围只有这一条纳管记录 —— 不补录、不牵连同一台 Connect
    /// 上的其它连接器。检测结论会回写 `inconsistent` 与 `inconsistent_detail`，
    /// 前端刷新后「不一致」标签和对比弹窗就能看到本次结果。
    ///
    /// * 远端存在：比库内配置里出现过的键（`name` 除外），有差异 → 标记，一致 → 清除；
    /// * 远端不存在（404）：置禁用并标记，快照只留原因，与整台 Connect 检测一致；
    /// * 取不到远端配置（Connect 不可达等）：报错，不改动任何库内标记。
    ///
    /// 不看运行状态：任务重启、rebalance 造成的抖动不该被判成配置不一致。
    #[log_call]
    pub async fn detect_connector(
        dto: DbzConnectorDetectDto,
    ) -> Result<Ro<DbzConnectorDetectVo>, anyhow::Error> {
        let node_type = dto.node_type.trim().to_lowercase();
        if !matches!(node_type.as_str(), "source" | "sink") {
            return Err(anyhow!("nodeType 只支持 source / sink"));
        }
        if dto.node_id.value() == 0 {
            return Err(anyhow!("nodeId 必须为正数"));
        }

        /// 一条纳管记录里检测要用到的字段，上游与下游取法一致
        struct DetectTarget {
            id: i64,
            kafka_connect_id: U64,
            name: String,
            connector_name: String,
            connector_config: String,
            inconsistent: bool,
            inconsistent_detail: Option<String>,
            enabled: bool,
        }

        let local = if node_type == "source" {
            let one = DbzSourceConnectorSvc::get_by_id::<DatabaseConnection>(dto.node_id, None)
                .await?
                .extra
                .ok_or_else(|| anyhow!("上游连接器<{}>不存在", dto.node_id.value()))?;
            DetectTarget {
                id: one.id.value() as i64,
                kafka_connect_id: one.kafka_connect_id,
                name: one.name,
                connector_name: one.connector_name,
                connector_config: one.connector_config,
                inconsistent: one.inconsistent,
                inconsistent_detail: one.inconsistent_detail,
                enabled: one.enabled,
            }
        } else {
            let one = DbzSinkConnectorSvc::get_by_id::<DatabaseConnection>(dto.node_id, None)
                .await?
                .extra
                .ok_or_else(|| anyhow!("下游连接器<{}>不存在", dto.node_id.value()))?;
            DetectTarget {
                id: one.id.value() as i64,
                kafka_connect_id: one.kafka_connect_id,
                name: one.name,
                connector_name: one.connector_name,
                connector_config: one.connector_config,
                inconsistent: one.inconsistent,
                inconsistent_detail: one.inconsistent_detail,
                enabled: one.enabled,
            }
        };

        let connect = Self::get_by_id::<DatabaseConnection>(local.kafka_connect_id, None)
            .await?
            .extra
            .ok_or_else(|| {
                anyhow!(
                    "连接器<{}>所属 Kafka Connect 不存在",
                    local.name
                )
            })?;
        if !connect.enabled {
            return Err(anyhow!(
                "所属 Kafka Connect<{}>已停用，无法检测；请先启用后再试",
                connect.name
            ));
        }
        let url = normalize_url(&connect.url);
        if url.is_empty() {
            return Err(anyhow!(
                "Kafka Connect<{}>未配置地址，无法检测",
                connect.name
            ));
        }

        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;
        let checked_at = now_ms();
        let config_valid = serde_json::from_str::<Value>(&local.connector_config)
            .map(|value| value.is_object())
            .unwrap_or(false);

        let (remote_exists, mismatch, detail_text, changed_keys, reason) =
            match KafkaConnectorUtils::fetch_connector_config(&url, &local.connector_name).await {
                Ok(remote) => {
                    let mismatch = config_mismatch(&local.connector_config, &remote);
                    let detail_text = compare_detail_text(
                        &local.connector_config,
                        &remote,
                        mismatch,
                        checked_at,
                        &local.name,
                    )?;
                    let DbzConnectorCompareDetail { rows, reason, .. } =
                        build_compare_detail(&local.connector_config, &remote, checked_at);
                    let changed_keys = rows
                        .into_iter()
                        .filter(|row| row.changed)
                        .map(|row| row.key)
                        .collect::<Vec<String>>();
                    (true, mismatch, detail_text, changed_keys, reason)
                }
                Err(KafkaConnectorError::NotFound) => (
                    false,
                    true,
                    missing_detail_text(checked_at, &local.name)?,
                    Vec::new(),
                    Some("检测时 Connect 上已不存在该连接器，已自动置为禁用".to_string()),
                ),
                Err(e) => {
                    return Err(anyhow!(
                        "读取连接器<{}>在 Connect 上的配置失败: {e}",
                        local.connector_name
                    ))
                }
            };

        let flag_changed = mismatch != local.inconsistent;
        let snapshot_changed = local.inconsistent_detail.as_deref() != detail_text.as_deref();
        // 远端不存在时还要置禁用并回写状态，即使标记已经是 true、快照也没变
        let need_disable = !remote_exists && local.enabled;
        let updated = flag_changed || snapshot_changed || need_disable;
        if updated {
            if node_type == "source" {
                let mut active = SourceActiveModel {
                    id: Set(local.id),
                    ..Default::default()
                };
                if flag_changed {
                    active.inconsistent = Set(mismatch);
                }
                active.inconsistent_detail = Set(detail_text.clone());
                active.updater_id = Set(0);
                if need_disable {
                    active.enabled = Set(false);
                    active.run_status = Set(ConnectorRunStatus::Disabled.value());
                    active.run_msg = Set(Some("检测：Connect 上已不存在该连接器".to_string()));
                }
                DbzSourceConnectorDao::update(active, db.as_ref())
                    .await
                    .map_err(|e| anyhow!("回写上游连接器<{}>检测标记失败: {e}", local.name))?;
            } else {
                let mut active = SinkActiveModel {
                    id: Set(local.id),
                    ..Default::default()
                };
                if flag_changed {
                    active.inconsistent = Set(mismatch);
                }
                active.inconsistent_detail = Set(detail_text.clone());
                active.updater_id = Set(0);
                if need_disable {
                    active.enabled = Set(false);
                    active.run_status = Set(ConnectorRunStatus::Disabled.value());
                    active.run_msg = Set(Some("检测：Connect 上已不存在该连接器".to_string()));
                }
                DbzSinkConnectorDao::update(active, db.as_ref())
                    .await
                    .map_err(|e| anyhow!("回写下游连接器<{}>检测标记失败: {e}", local.name))?;
            }
        }

        let changed = changed_keys.len() as u32;
        info!(
            "{}连接器<{}>(id={}) 一致性检测完成: exists={}, inconsistent={}, changed={}, updated={}",
            if node_type == "source" { "上游" } else { "下游" },
            local.name,
            local.id,
            remote_exists,
            mismatch,
            changed,
            updated
        );
        let msg = if !remote_exists {
            format!("检测完成：Connect 上已不存在「{}」，记录已置为禁用并标记不一致", local.name)
        } else if mismatch {
            format!(
                "检测完成：「{}」有 {} 处配置不一致，已刷新检测标记与对比快照",
                local.name, changed
            )
        } else {
            format!("检测完成：「{}」库内配置与 Connect 实际一致，标记已清除", local.name)
        };
        Ok(Ro::success(msg).extra(Some(DbzConnectorDetectVo {
            node_type,
            connector_name: local.connector_name,
            config_valid,
            remote_exists,
            inconsistent: mismatch,
            inconsistent_detail: detail_text,
            changed,
            changed_keys,
            reason,
            checked_at,
            updated,
        })))
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

        // broker 先查一轮：连接器能不能正常投递取决于它后面的集群。这一步只回写
        // 集群状态、不参与下面的告警收敛 —— broker 不通时受影响的连接器会在各自
        // 的检查里报出来，再单独为 broker 报一遍等于同一根因刷两条。
        if let Err(e) = DbzKafkaBrokerSvc::check_all().await {
            warn!("检查 Kafka 集群状态失败: {e}");
        }

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
            Self::update_status(kc, KafkaConnectStatus::Disabled, None).await?;
            for source in sources {
                DbzSourceConnectorSvc::update_run_status(
                    source,
                    ConnectorRunStatus::Disabled,
                    None,
                )
                .await?;
            }
            for sink in sinks {
                DbzSinkConnectorSvc::update_run_status(sink, ConnectorRunStatus::Disabled, None)
                    .await?;
            }
            return Ok(errors);
        }

        // 集群自身的连通性每轮都探测一次：即使暂时没有连接器，记录上的状态也应当
        // 反映「这台集群现在通不通」，否则新建的集群会一直停在「无状态」。
        // 连不上时把探测到的原因留下来，挂在上面的连接器共用同一句结论。
        let connect_error: Option<String> = if url.is_empty() {
            warn!("{kc_label}: 未配置地址，按连接失败处理");
            Some("未配置 Kafka Connect 地址".to_string())
        } else {
            match KafkaConnectorUtils::check_connect(&url).await {
                Ok(()) => None,
                Err(e) => {
                    warn!("{kc_label}: 连接失败: {e}");
                    Some(format!("{e:#}"))
                }
            }
        };

        if let Some(reason) = connect_error {
            Self::update_status(kc, KafkaConnectStatus::ConnectFailed, Some(reason.clone()))
                .await?;
            // 集群自身也算一个异常对象：名下没有连接器时，不记这一条，整轮会汇总成
            // 「一切正常」，可运维其实连不上这台集群。
            errors.push(format!(
                "{kc_label}: {}",
                KafkaConnectStatus::ConnectFailed.value()
            ));
            let status = ConnectorRunStatus::Unreachable;
            // 连接器的数据线上游，真正的根因在这台集群上；这里只说清「为什么没连上」，
            // 不把整段探测错误复制到每条连接器上（集群那一行已经有完整原因）
            let reason = Some(format!("所属 Kafka Connect 不可达：{reason}"));
            // 已经在上面报过告警的上游，用来判断下游是否还需要单独告警
            let alarmed_sources: HashSet<u64> = enabled_sources
                .iter()
                .map(|source| source.id.value())
                .collect();
            for source in &enabled_sources {
                DbzSourceConnectorSvc::update_run_status(source, status, reason.clone()).await?;
                Self::send_source_alarm(source, kc, status).await;
                errors.push(format!(
                    "上游连接器<{}>(id={}): {}",
                    source.name,
                    source.id.value(),
                    status.value()
                ));
            }
            for sink in &enabled_sinks {
                DbzSinkConnectorSvc::update_run_status(sink, status, reason.clone()).await?;
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

        Self::update_status(kc, KafkaConnectStatus::Normal, None).await?;

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
                    DbzSourceConnectorSvc::update_run_status(
                        source,
                        ConnectorRunStatus::Running,
                        None,
                    )
                    .await?
                }
                Err(fault) => {
                    DbzSourceConnectorSvc::update_run_status(
                        source,
                        fault.status,
                        fault.reason.clone(),
                    )
                    .await?;
                    Self::send_source_alarm(source, kc, fault.status).await;
                    // 告警先发，再修复：自动修复不替代报警
                    Self::apply_repair(&url, &source.connector_name, fault.action).await;
                    errors.push(format!("{label}: {}", fault.status.value()));
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
                    DbzSinkConnectorSvc::update_run_status(sink, ConnectorRunStatus::Running, None)
                        .await?
                }
                Err(fault) => {
                    DbzSinkConnectorSvc::update_run_status(
                        sink,
                        fault.status,
                        fault.reason.clone(),
                    )
                    .await?;
                    Self::send_sink_alarm(sink, sources_by_id, kc, fault.status).await;
                    // 告警先发，再修复：自动修复不替代报警
                    Self::apply_repair(&url, &sink.connector_name, fault.action).await;
                    errors.push(format!("{label}: {}", fault.status.value()));
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
    /// 若检查失败、部署失败或状态异常，返回对应的状态、修复动作与失败原因。
    ///
    /// 原因取自 Kafka Connect 的响应：部署被拒时是 HTTP 响应体（配置校验会指出哪个
    /// 参数不合法），状态异常时是异常 trace 的首行。写库后前端悬停即可看到，不必
    /// 再去翻服务端日志。
    ///
    /// ## 返回值
    /// * `Ok(())` - 连接器及其任务都正常运行
    /// * `Err(ConnectorFault)` - 连接器异常，附带要回写的状态、修复动作与原因
    async fn ensure_and_check_connector(
        check: &KafkaConnectorCheck<'_>,
    ) -> Result<(), ConnectorFault> {
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
                ConnectorFault::new(ConnectorRunStatus::Unreachable, RepairAction::None)
                    .with_reason(format!("检查连接器是否存在失败: {e}"))
            })?;

        if !exists {
            // 配置开了 kafka 信号通道却没填 broker 地址：Debezium 在这个键上没有默认值，
            // 老版本直接在建 task 时 NPE，报错里看不出跟这个键有关。先自己拦下来，
            // 别让它去撞一次 Connect 再回一段 Java 调用栈。
            if DbzSignalUtils::kafka_bootstrap_missing(connector_config) {
                warn!(
                    "{label}: 配置开了 kafka 信号通道但没有 signal.kafka.bootstrap.servers，跳过部署"
                );
                return Err(
                    ConnectorFault::new(ConnectorRunStatus::Failed, RepairAction::None)
                        .with_reason(
                            "配置启用了 kafka 信号通道，但 signal.kafka.bootstrap.servers 为空：\
                     Debezium 在这个键上没有默认值，留空会让连接器起不来。\
                     在页面上保存一次配置，会自动填成关联 Kafka 集群的地址",
                        ),
                );
            }
            info!("{label}: 连接器<{connector_name}>不存在，开始部署...");
            KafkaConnectorUtils::deploy_connector(base_url, connector_name, connector_config)
                .await
                .map_err(|e| {
                    warn!("{label}: 部署连接器<{connector_name}>失败: {e}");
                    ConnectorFault::new(ConnectorRunStatus::Failed, RepairAction::None)
                        .with_reason(format!("部署连接器失败: {e}"))
                })?;
            info!("{label}: 连接器<{connector_name}>部署成功");
        }

        // 查询连接器状态（含任务状态）。刚部署的连接器会短暂处于 UNASSIGNED，
        // 这时判异常会误告警并触发无意义的重启，先等它过渡到实际状态。
        let health = Self::fetch_status(base_url, connector_name, label, !exists)
            .await
            .map_err(|e| {
                warn!("{label}: 查询连接器<{connector_name}>状态失败: {e}");
                ConnectorFault::new(ConnectorRunStatus::Unreachable, RepairAction::None)
                    .with_reason(format!("查询连接器状态失败: {e}"))
            })?;

        if health.state == KafkaConnectorStatus::Running {
            return Ok(());
        }

        // 连接器自身 RUNNING、任务挂了：只重启这几个任务
        if !health.failed_tasks.is_empty() {
            warn!(
                "{label}: 连接器<{connector_name}>有 {} 个任务异常: {:?}",
                health.failed_tasks.len(),
                health.failed_tasks
            );
            let fault = ConnectorFault::new(
                ConnectorRunStatus::Failed,
                RepairAction::RestartTasks(health.failed_tasks),
            );
            return Err(match health.reason {
                Some(reason) => fault.with_reason(reason),
                None => fault,
            });
        }

        if health.state == KafkaConnectorStatus::Unassigned {
            warn!(
                "{label}: 连接器<{connector_name}>返回未知状态: {}",
                health.state
            );
            return Err(ConnectorFault::new(
                ConnectorRunStatus::Failed,
                RepairAction::RestartConnector,
            )
            .with_reason(format!("连接器返回未知状态: {}", health.state)));
        }

        warn!(
            "{label}: 连接器<{connector_name}>状态异常: {}",
            health.state
        );
        // 只有 failed 有确定的修复动作；paused / stopped 多为人工维护、destroyed 只能重建，都不动
        let paused = matches!(
            health.state,
            KafkaConnectorStatus::Paused | KafkaConnectorStatus::Stopped
        );
        let action = if health.state == KafkaConnectorStatus::Failed {
            RepairAction::RestartConnector
        } else {
            RepairAction::None
        };
        let fault = ConnectorFault::new(
            if paused {
                ConnectorRunStatus::Stopped
            } else {
                ConnectorRunStatus::Failed
            },
            action,
        );
        Err(match health.reason {
            // 有 trace 时用 trace：它说明了「为什么 FAILED」
            Some(reason) => fault.with_reason(reason),
            None => fault.with_reason(format!("连接器状态异常: {}", health.state)),
        })
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
    ) -> Result<ConnectorHealth, KafkaConnectorError> {
        let mut result =
            KafkaConnectorUtils::fetch_connector_status(base_url, connector_name).await?;
        if !just_deployed {
            return Ok(result);
        }

        for _ in 0..DEPLOY_STATUS_RETRIES {
            if result.state != KafkaConnectorStatus::Unassigned {
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

    /// # 更新集群记录的状态与检查结论
    ///
    /// **状态和结论都没变就不写库**：检查每轮都在跑，状态常年是「正常」，每轮写
    /// 一次只会让 `update_ms` 失去意义。结论变了要写——一直是「连接失败」但原因从
    /// 「拒绝连接」变成「HTTP 401」时，正是最该让人看到的时刻。
    ///
    /// ## 参数
    /// * `kc` - 库里的记录（用来比对是否真的有变化）
    /// * `run_status` - 检查得出的状态
    /// * `run_msg` - 失败原因（正常时传 `None`，库里会被清空）
    async fn update_status(
        kc: &DbzKafkaConnectVo,
        run_status: KafkaConnectStatus,
        run_msg: Option<String>,
    ) -> Result<(), anyhow::Error> {
        if kc.run_status == run_status && kc.run_msg == run_msg {
            return Ok(());
        }

        let mut modify_dto = DbzKafkaConnectModifyDto::builder()
            .id(kc.id)
            .run_status(run_status)
            ._current_user_id(U64(1))
            ._current_ms(U64(now_ms()))
            .build();
        // 直接赋值：字段是 Option<Option<String>>，Some(None) 才是「置 NULL」
        modify_dto.run_msg = Some(run_msg);

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
    ///
    /// 这两种场景下状态是被集群配置变化推着走的，上一轮的失败原因已经不再适用，
    /// 所以清掉 `run_msg`。
    async fn set_connectors_status(
        kafka_connect_id: U64,
        run_status: ConnectorRunStatus,
    ) -> Result<(), SvcError> {
        let only_enabled = run_status == ConnectorRunStatus::Unknown;
        for source in Self::list_sources_of(kafka_connect_id).await? {
            if only_enabled && !source.enabled {
                continue;
            }
            DbzSourceConnectorSvc::update_run_status(&source, run_status, None).await?;
        }
        for sink in Self::list_sinks_of(kafka_connect_id).await? {
            if only_enabled && !sink.enabled {
                continue;
            }
            DbzSinkConnectorSvc::update_run_status(&sink, run_status, None).await?;
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

    /// # 统计挂在该 broker 上的 Kafka Connect 数量
    ///
    /// 删 broker 之前用：还有 Connect 记录指着它时不允许删（数据库侧是 restrict）。
    pub(crate) async fn count_by_kafka_broker(kafka_broker_id: U64) -> Result<usize, SvcError> {
        let query_dto = DbzKafkaConnectQueryDto::builder()
            .kafka_broker_id(kafka_broker_id)
            .build();
        Ok(
            Self::list_by_query_dto::<DatabaseConnection>(query_dto, None)
                .await?
                .extra
                .unwrap_or_default()
                .len(),
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

/// 单个配置项的比对中间结果
struct ComparedEntry {
    key: String,
    /// 库内值；只在远端存在该项时为 `None`
    local: Option<Value>,
    /// 远端值；远端缺少该项时为 `None`
    remote: Option<Value>,
    /// 是否只在远端存在（库内没有纳管这一项）
    remote_only: bool,
}

impl ComparedEntry {
    /// 与 {@link config_mismatch} 完全同源的差异判定：值不同，或只有一侧存在
    fn changed(&self) -> bool {
        match (&self.local, &self.remote) {
            (Some(local), Some(remote)) => !json_eq(local, remote),
            // 只有一侧存在：远端缺键，或远端多出一项而库内没纳管
            _ => true,
        }
    }
}

/// # 逐项列出参与比对的配置
///
/// 口径与检测一致，两侧的键集合都参与（都排除 `name`，它是记录名）：
///
/// * 库内出现的键 —— 远端缺键或值不同即为差异；
/// * 只在远端出现的键 —— 库内没有纳管这一项，同样是差异。
///
/// 远端独有项必须列出来，而且必须算差异：它正式意味着「拿库内配置重建不出这个
/// 连接器」（有人在 Connect 上直接加过配置，或者插件升级换了键名）。只展示库内键
/// 会让人把「远端已有等价的新键」读成「远端少配了几项」，判定也会漏掉这类漂移。
///
/// 这些行排在库内键之后、`remote_only` 为真：下发会以库内为基准整体覆盖远端，
/// 所以它们只能随同步一起消失，不能逐项编辑。
///
/// 库内配置或远端配置不是合法 JSON 对象时返回 `None`（此时检测判定为不一致）。
fn compare_entries(local_config: &str, remote_config: &Value) -> Option<Vec<ComparedEntry>> {
    let local = serde_json::from_str::<Value>(local_config).ok()?;
    let local_obj = local.as_object()?;
    let remote_obj = remote_config.as_object()?;
    let mut entries: Vec<ComparedEntry> = local_obj
        .iter()
        .filter(|(key, _)| key.as_str() != "name")
        .map(|(key, local)| ComparedEntry {
            key: key.clone(),
            local: Some(local.clone()),
            remote: remote_obj.get(key).cloned(),
            remote_only: false,
        })
        .collect();
    // 字段顺序稳定：按键名排序，与库内 JSON 的书写顺序无关
    entries.sort_by(|a, b| a.key.cmp(&b.key));

    let mut remote_only: Vec<ComparedEntry> = remote_obj
        .iter()
        .filter(|(key, _)| key.as_str() != "name" && !local_obj.contains_key(*key))
        .map(|(key, remote)| ComparedEntry {
            key: key.clone(),
            local: None,
            remote: Some(remote.clone()),
            remote_only: true,
        })
        .collect();
    remote_only.sort_by(|a, b| a.key.cmp(&b.key));
    // 库内键在前、远端独有项在后，两类差异一眼分得开
    entries.extend(remote_only);
    Some(entries)
}

pub(crate) fn config_mismatch(local_config: &str, remote_config: &Value) -> bool {
    match compare_entries(local_config, remote_config) {
        Some(entries) => entries.iter().any(ComparedEntry::changed),
        None => true,
    }
}

/// 敏感配置项：命中的值一律脱敏，不落库明文、不回显
const SENSITIVE_KEY_PARTS: &[&str] = &[
    "password",
    "passwd",
    "secret",
    "token",
    "credential",
    "passphrase",
    "private.key",
    "access.key",
    "api.key",
    "apikey",
    "sasl.jaas.config",
    "basic.auth.user.info",
    "ssl.keystore",
    "ssl.truststore",
];

/// 脱敏后的占位文案（未变化）
const MASKED: &str = "***（已脱敏）";
/// 脱敏且恰好是差异项：既不复述密钥，又能看出「这里变了」
const MASKED_CHANGED: &str = "***（已变更，已脱敏）";

/// 键名是否属于敏感配置（大小写不敏感，按子串匹配）
pub(crate) fn is_sensitive_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    SENSITIVE_KEY_PARTS.iter().any(|part| key.contains(part))
}

/// 单值展示：字符串原样，对象/数组按格式化 JSON，其它走紧凑 JSON
fn json_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => serde_json::to_string_pretty(other).unwrap_or_else(|_| other.to_string()),
    }
}

/// # 生成不一致对比快照
///
/// 与 {@link config_mismatch} 同源，保证「标签说不一致」时弹窗一定能看出差异；
/// 敏感项值统一脱敏，差异标记只保留「变了/没变」。
fn build_compare_detail(
    local_config: &str,
    remote_config: &Value,
    checked_at: u64,
) -> DbzConnectorCompareDetail {
    let Some(entries) = compare_entries(local_config, remote_config) else {
        return DbzConnectorCompareDetail {
            checked_at,
            remote_exists: true,
            reason: Some("库内配置或远端配置不是合法 JSON 对象，无法逐项展示".to_string()),
            rows: Vec::new(),
        };
    };
    let rows = entries
        .into_iter()
        .map(|entry| {
            let changed = entry.changed();
            let sensitive = is_sensitive_key(&entry.key);
            let masked = if changed { MASKED_CHANGED } else { MASKED };
            let local = entry.local.as_ref().map(|value| {
                if sensitive {
                    masked.to_string()
                } else {
                    json_text(value)
                }
            });
            let remote = entry.remote.as_ref().map(|value| {
                if sensitive {
                    masked.to_string()
                } else {
                    json_text(value)
                }
            });
            DbzConnectorCompareRow {
                key: entry.key,
                local,
                remote,
                changed,
                sensitive,
                remote_only: entry.remote_only,
            }
        })
        .collect();
    DbzConnectorCompareDetail {
        checked_at,
        remote_exists: true,
        reason: None,
        rows,
    }
}

/// 把对比快照序列化进 `inconsistent_detail`；一致时写入 `None` 清空快照
pub(crate) fn compare_detail_text(
    local_config: &str,
    remote_config: &Value,
    mismatch: bool,
    checked_at: u64,
    connector_name: &str,
) -> Result<Option<String>, anyhow::Error> {
    if !mismatch {
        return Ok(None);
    }
    let detail = build_compare_detail(local_config, remote_config, checked_at);
    serde_json::to_string(&detail)
        .map(Some)
        .map_err(|e| anyhow!("序列化连接器<{connector_name}>对比快照失败: {e}"))
}

/// 远端已不存在时的快照：不展示空对比，只说明原因
fn missing_detail_text(
    checked_at: u64,
    connector_name: &str,
) -> Result<Option<String>, anyhow::Error> {
    let detail = DbzConnectorCompareDetail {
        checked_at,
        remote_exists: false,
        reason: Some("检测时 Connect 上已不存在该连接器，已自动置为禁用".to_string()),
        rows: Vec::new(),
    };
    serde_json::to_string(&detail)
        .map(Some)
        .map_err(|e| anyhow!("序列化连接器<{connector_name}>对比快照失败: {e}"))
}

/// # 文本按数值解析
///
/// 连接器配置里同一个端口常常一侧写成字符串、一侧写成数值（库内 `"3306"`、
/// 远端 `3306`）。能解析成有限数值才参与数值比较，其余（空串、表名、`true`
/// 之类）保持原样按值比较，不会被误判成相等。
fn text_number(value: &str) -> Option<f64> {
    let text = value.trim();
    if text.is_empty() {
        return None;
    }
    text.parse::<f64>().ok().filter(|item| item.is_finite())
}

/// # 递归比较两个 JSON 值
///
/// 对象键顺序无关、数组逐项、数字按数值比较（`1` 与 `1.0` 视为相同）；
/// 文本与数字能解析成同一数值时也视为相同（`"3306"` 与 `3306`、`"4"` 与 `4`），
/// 其余按值相等。口径与前端「不一致」标签、对比弹窗完全同源。
fn json_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len()
                && x.iter()
                    .all(|(k, v)| y.get(k).map(|w| json_eq(v, w)).unwrap_or(false))
        }
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y.iter()).all(|(v, w)| json_eq(v, w))
        }
        (Value::Number(x), Value::Number(y)) => x.as_f64() == y.as_f64(),
        // 文本 vs 数字：文本是数字才比较数值，否则类型不同就是不同
        (Value::String(x), Value::Number(y)) | (Value::Number(y), Value::String(x)) => {
            match (text_number(x), y.as_f64()) {
                (Some(left), Some(right)) => left == right,
                _ => false,
            }
        }
        _ => a == b,
    }
}

/// # 取上游连接器的 topic 前缀
///
/// Debezium 新版用 `topic.prefix`，老版用 `database.server.name`；两者取到即用。
fn source_prefix(connector_config: &str) -> Option<String> {
    let value: Value = serde_json::from_str(connector_config).ok()?;
    let obj = value.as_object()?;
    let prefix = obj
        .get("topic.prefix")
        .or_else(|| obj.get("database.server.name"))
        .and_then(|v| v.as_str())?
        .trim();
    if prefix.is_empty() {
        None
    } else {
        Some(prefix.to_string())
    }
}

/// # 按下游连接器的 topics 反推上游连接器
///
/// 下游配置里的 `topics` 是上游按 `<prefix>.<表名>` 产出的 topic。逐个上游比对
/// 前缀，**只有唯一命中才自动补录**：命中多个时无法确定归属，留给人工选择。
///
/// ## 参数
/// * `remote_config` - 下游连接器的远端配置
/// * `sources` - 该集群下已有的上游连接器
fn resolve_upstream(
    remote_config: &Value,
    sources: &[DbzSourceConnectorVo],
) -> Option<i64> {
    let obj = remote_config.as_object()?;
    let topics: Vec<String> = match obj.get("topics") {
        Some(Value::String(text)) => text
            .split(',')
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty())
            .collect(),
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|v| v.as_str())
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty())
            .collect(),
        _ => return None,
    };
    if topics.is_empty() {
        return None;
    }
    let mut matched: Vec<i64> = Vec::new();
    for source in sources {
        let Some(prefix) = source_prefix(&source.connector_config) else {
            continue;
        };
        let marker = format!("{prefix}.");
        if topics.iter().any(|topic| topic.starts_with(&marker)) {
            let id = source.id.value() as i64;
            if !matched.contains(&id) {
                matched.push(id);
            }
        }
    }
    if matched.len() == 1 {
        matched.first().copied()
    } else {
        None
    }
}

/// # 记录一条检测结果并累加计数
fn push_detect_item(
    out: &mut DbzKafkaConnectDetectVo,
    connector_name: &str,
    connector_type: &str,
    action: &str,
    message: Option<String>,
) {
    match action {
        "added" => out.added += 1,
        "marked" => out.marked += 1,
        "disabled" => out.disabled += 1,
        "cleared" => out.cleared += 1,
        "unchanged" => out.unchanged += 1,
        _ => out.skipped += 1,
    }
    out.items.push(DbzConnectorDetectItemVo {
        connector_name: connector_name.to_string(),
        connector_type: connector_type.to_string(),
        action: action.to_string(),
        message,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// 端口一类配置常被一方写成字符串、另一方写成数值：同一个数不能算差异
    #[test]
    fn numeric_text_equals_number() {
        assert!(json_eq(&json!("3306"), &json!(3306)));
        assert!(json_eq(&json!(3306), &json!("3306")));
        assert!(json_eq(&json!(" 3306 "), &json!(3306)));
        assert!(json_eq(&json!("4"), &json!(4.0)));
        // 数值后缀的写法同样按数值比较
        assert!(json_eq(&json!("9092"), &json!(9.092e3)));
    }

    /// 只有「文本能解析成数字」才走数值比较，其余类型差异仍算不一致
    #[test]
    fn non_numeric_text_stays_different() {
        assert!(!json_eq(&json!("3307"), &json!(3306)));
        assert!(!json_eq(&json!("orders"), &json!(3306)));
        assert!(!json_eq(&json!(""), &json!(0)));
        assert!(!json_eq(&json!("true"), &json!(true)));
        assert!(!json_eq(&json!("4 "), &json!(4.5)));
    }

    /// 嵌套结构沿用同一口径：对象键序无关，数组逐项比
    #[test]
    fn nested_values_follow_same_rule() {
        assert!(json_eq(
            &json!({"database.port": "3306", "tasks.max": 4}),
            &json!({"tasks.max": "4", "database.port": 3306}),
        ));
        assert!(json_eq(&json!(["1", 2]), &json!([1, "2"])));
        assert!(!json_eq(&json!({"tasks.max": 4}), &json!({"tasks.max": 5})));
    }

    /// 卡片检测与整台检测共用判定：库内键的值不同才标记，changedKeys 与快照同源
    #[test]
    fn single_detect_reports_only_changed_local_keys() {
        let local = r#"{"connector.class":"MySqlConnector","database.hostname":"10.0.0.9","table.include.list":"dbz.orders_archive","database.password":"dbz-secret"}"#;
        let remote = json!({
            "connector.class": "MySqlConnector",
            "database.hostname": "10.0.0.11",
            "table.include.list": "dbz.orders",
            "database.password": "dbz-secret"
        });
        assert!(config_mismatch(local, &remote));

        let detail = build_compare_detail(local, &remote, 1760000000000);
        let changed_keys = detail
            .rows
            .iter()
            .filter(|row| row.changed)
            .map(|row| row.key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(changed_keys, vec!["database.hostname", "table.include.list"]);
        // 敏感项只给键名，不回显明文
        let password = detail
            .rows
            .iter()
            .find(|row| row.key == "database.password")
            .expect("库内键都应出现在快照里");
        assert!(password.sensitive);
        assert!(!password.local.as_deref().unwrap_or_default().contains("dbz-secret"));

        let text = compare_detail_text(local, &remote, true, 1760000000000, "订单库增量")
            .expect("快照序列化不应失败")
            .expect("不一致时必须有快照");
        let parsed = serde_json::from_str::<DbzConnectorCompareDetail>(&text)
            .expect("inconsistentDetail 必须是可解析的对比快照");
        assert!(parsed.remote_exists);
        assert_eq!(parsed.rows.len(), detail.rows.len());
    }

    /// 只在远端出现的键同样算差异，并且必须出现在快照里
    ///
    /// 这类键意味着「拿库内配置重建不出这个连接器」：插件升级换键名（`http.api.url`
    /// → `http.url`）、有人在 Connect 上直接加过配置，都属于这种情况。藏起来会让人
    /// 把「远端已有等价的新键」误读成「远端少配了几项」。
    #[test]
    fn remote_only_keys_are_reported_in_snapshot() {
        let local = r#"{"connector.class":"MySqlConnector","database.port":"3306"}"#;
        let remote = json!({
            "connector.class": "MySqlConnector",
            "database.port": 3306,
            "table.include.list": "dbz.orders"
        });
        assert!(config_mismatch(local, &remote), "库内没纳管的远端键同样是不一致");

        let detail = build_compare_detail(local, &remote, 1);
        // 库内键在前、远端独有项在最后，顺序稳定
        let keys = detail
            .rows
            .iter()
            .map(|row| row.key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            vec!["connector.class", "database.port", "table.include.list"]
        );
        // 值相同的库内项不算差异
        assert!(detail.rows[..2].iter().all(|row| !row.changed));
        let extra = detail.rows.last().expect("远端独有项必须在快照里");
        assert!(extra.remote_only);
        assert!(extra.changed);
        assert_eq!(extra.local, None, "库内没有这一项，值为空而不是空字符串");
        assert_eq!(extra.remote.as_deref(), Some("dbz.orders"));
    }

    /// 远端独有项也不回显明文：脱敏口径与库内键一致
    #[test]
    fn remote_only_sensitive_keys_stay_masked() {
        let local = r#"{"connector.class":"MySqlConnector"}"#;
        let remote = json!({
            "connector.class": "MySqlConnector",
            "database.password": "dbz-secret"
        });
        let detail = build_compare_detail(local, &remote, 1);
        let extra = detail.rows.last().expect("远端独有敏感项也要列出来");
        assert!(extra.remote_only);
        assert!(extra.sensitive);
        assert!(!extra.remote.as_deref().unwrap_or_default().contains("dbz-secret"));
    }

    /// 库内配置不是合法 JSON 对象时标记不一致，但快照只留原因、不列伪差异
    #[test]
    fn broken_local_config_marks_inconsistent_without_rows() {
        let local = "not-a-json-object";
        let remote = json!({"connector.class": "MySqlConnector"});
        assert!(config_mismatch(local, &remote));
        let detail = build_compare_detail(local, &remote, 1);
        assert!(detail.rows.is_empty());
        assert!(detail.reason.is_some());
    }
}
