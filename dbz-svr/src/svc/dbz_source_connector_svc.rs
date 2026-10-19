//! # 上游连接器服务
//!
//! 上游（source）连接器是 Debezium 从源库读取变更并写入 Kafka 的连接器；一条
//! `dbz_source_connector` 记录对应 Kafka Connect 上的一个连接器实例，并保存该
//! Kafka Connect 的地址。下游（sink）连接器通过 `source_connector_id` 归属于它，
//! 见 [`DbzSinkConnectorSvc`](crate::svc::DbzSinkConnectorSvc)。
//!
//! 健康检查以「上游连接器」为入口：先探测 Kafka Connect 连通性，再检查上游连接器
//! 本身，最后逐个检查挂在该上游下的下游连接器，各自回写各自的运行状态、各自告警。
//!
//! 由于下游可以独立于上游启用（上游被禁用时其连接器被注销，下游仍跑在 Kafka
//! Connect 上），检查范围是「已启用的上游」加上「名下还有已启用下游的上游」，被
//! 禁用且没有启用下游的记录直接跳过。

use crate::dic::{ConnectorRunStatus, ConnectorStatus};
use crate::dto::DbzSinkConnectorQueryDto;
use crate::svc::DbzSinkConnectorSvc;
use crate::utils::{ConnectorError, DbzAlarmUtils, DbzConnectorUtils};
use crate::vo::DbzSinkConnectorVo;
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

/// # 归一化 Kafka Connect 地址
///
/// 去掉首尾空白与末尾 `/`。既用作请求基地址，也用于判断两个地址是否指向同一台
/// 集群（改地址前要先确认「是否真的换了集群」）。
fn normalize_url(url: &str) -> String {
    url.trim().trim_end_matches('/').to_string()
}

#[svc(
    before_add = DbzSourceConnectorSvc::before_add_hook,
    before_modify = DbzSourceConnectorSvc::before_modify_hook,
    before_del_by_id = DbzSourceConnectorSvc::before_del_by_id_hook
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

// SvcError 由框架定义且体积较大，而钩子/服务方法的返回类型必须与宏约定一致，
// 无法改成 Box 包裹，这里统一放行该 lint
#[allow(clippy::result_large_err)]
impl DbzSourceConnectorSvc {
    /// # 删除前钩子：还有下游则拒绝；否则先注销连接器再删记录
    ///
    /// 两件事：
    ///
    /// 1. **下游拦截**：数据库侧已经是 `on delete restrict`，这里提前拦一道，把
    ///    外键错误换成可读的提示。下游是否启用不影响判断：外键约束不看 `enabled`。
    /// 2. **注销连接器**：记录一删，这个连接器就再没有任何地方管理它 —— 健康检查
    ///    找不到它，不会告警也不会自动修复，它会一直跑下去。所以必须先注销。注销
    ///    失败则拒绝删除：宁可这次删不掉，也不留一个无人管理的连接器。
    ///
    /// ## 参数
    /// * `id` - 待删除的上游连接器ID
    ///
    /// ## 返回值
    /// * `Ok(U64)` - 连接器已注销（或本来就不存在），原样返回 ID
    /// * `Err(SvcError)` - 该上游下仍有下游连接器，或注销连接器失败
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

        let Some(one) = Self::get_by_id::<DatabaseConnection>(id, None).await?.extra else {
            return Ok(id);
        };
        let url = normalize_url(&one.kafka_connect_url);
        // 地址为空说明从未部署过，没有可注销的东西
        if !url.is_empty() {
            info!(
                "上游连接器<{}>(id={}): 删除记录前注销连接器<{}>",
                one.name,
                one.id.value(),
                one.connector_name
            );
            DbzConnectorUtils::delete_connector(&url, &one.connector_name).await?;
        }
        Ok(id)
    }

    /// # 新增前钩子：新建即禁用时直接落为「已禁用」
    ///
    /// 被禁用的记录不参与健康检查，不在这里写状态就会一直停在「无状态」。
    async fn before_add_hook(
        mut add_dto: DbzSourceConnectorAddDto,
    ) -> Result<DbzSourceConnectorAddDto, SvcError> {
        if add_dto.enabled == Some(false) {
            add_dto.run_status = Some(ConnectorRunStatus::Disabled);
        }
        Ok(add_dto)
    }

    /// # 修改前钩子：连接器搬家、下发配置、禁用注销与状态同步
    ///
    /// 由 `#[svc]` 宏在 `modify` 写库**之前**调用。记录上的改动必须同步到
    /// Kafka Connect，否则就只是改了库：
    ///
    /// 1. **连接器搬家**：Kafka Connect 上的连接器不会随记录迁移。改了
    ///    `kafka_connect_url` 或 `connector_name` 以后，旧地址上的旧连接器仍在抓
    ///    源库变更，而紧接着的下发会在新位置再建一个 —— 两个连接器同时抓同一份
    ///    变更，下游会收到两份。所以先按「旧地址 + 旧名字」注销，再写库。
    /// 2. **下发**：健康检查只在连接器「不存在」时才部署，改了 `connector_config`
    ///    若不主动下发就永远不会生效。这里按新配置 PUT 一次（`PUT
    ///    /connectors/{name}/config` 对已存在的是更新、对不存在的是创建），搬家
    ///    时也顺带在新位置建好，不必等下一轮检查。配置非法则请求失败，不会把一条
    ///    跑不起来的配置留在库里。
    /// 3. **禁用**：先注销连接器再写库。注销失败则整个修改请求失败，宁可禁用不成，
    ///    也不允许出现「库里已禁用、连接器还在跑」。
    /// 4. **状态**：禁用置「已禁用」；重新启用、搬家或重新下发配置置「无状态」，
    ///    等下一轮检查判定。开关只有**真的变化**才改写 —— 普通编辑常会带上原有的
    ///    `enabled`，不能因此把检查得出的状态覆盖掉。
    ///
    /// 注销与下发都在写库前；若随后写库失败，记录仍指向旧位置，下一轮健康检查会
    /// 把连接器重新部署回去，可自愈。
    ///
    /// 注：不级联处理下游连接器 —— 它们各自有 `enabled` 开关；但上游被禁用后其
    /// 下游仍在健康检查范围内（见 [`check_all_connectors`](Self::check_all_connectors)），
    /// 要彻底停止下游请单独禁用对应的下游连接器。
    ///
    /// ## 返回值
    /// * `Ok(ModifyDto)` - 补好 `run_status` 的修改参数
    /// * `Err(SvcError)` - 查询原记录失败，或注销/下发配置失败
    async fn before_modify_hook(
        mut modify_dto: DbzSourceConnectorModifyDto,
    ) -> Result<DbzSourceConnectorModifyDto, SvcError> {
        let Some(id) = modify_dto.id else {
            return Ok(modify_dto);
        };
        // 健康检查只回写 run_status，下面几个判断都用不上，跳过这次查询
        if modify_dto.enabled.is_none()
            && modify_dto.kafka_connect_url.is_none()
            && modify_dto.connector_name.is_none()
            && modify_dto.connector_config.is_none()
        {
            return Ok(modify_dto);
        }
        let Some(old) = Self::get_by_id::<DatabaseConnection>(id, None).await?.extra else {
            return Ok(modify_dto);
        };

        let old_url = normalize_url(&old.kafka_connect_url);
        let new_url = modify_dto
            .kafka_connect_url
            .as_deref()
            .map(normalize_url)
            .unwrap_or_else(|| old_url.clone());
        let new_name = modify_dto
            .connector_name
            .as_deref()
            .map(str::trim)
            .unwrap_or(&old.connector_name)
            .to_string();
        let url_changed = new_url != old_url;
        let name_changed = new_name != old.connector_name;
        let relocated = url_changed || name_changed;
        let config_changed = modify_dto
            .connector_config
            .as_deref()
            .is_some_and(|config| config != old.connector_config);
        let disabling = modify_dto.enabled == Some(false) && old.enabled;
        let enabled_after = modify_dto.enabled.unwrap_or(old.enabled);

        // 旧地址为空说明这台集群上从未部署过连接器，没有可注销的东西，直接放行
        if !old_url.is_empty() && (relocated || disabling) {
            info!(
                "上游连接器<{}>(id={}): 旧连接器<{}>在 <{old_url}> 上，先注销再写库",
                old.name,
                old.id.value(),
                old.connector_name
            );
            DbzConnectorUtils::delete_connector(&old_url, &old.connector_name).await?;
        }

        // 配置改动或搬家后主动下发一次，避免等下一轮检查（最长一个检查间隔）。
        // 禁用状态下不下发，否则会把连接器重新创建出来，与「已禁用」矛盾。
        if enabled_after && (config_changed || relocated) && !new_url.is_empty() {
            let config = modify_dto
                .connector_config
                .clone()
                .unwrap_or_else(|| old.connector_config.clone());
            info!(
                "上游连接器<{}>(id={}): 位置或配置已变更，下发到 <{new_url}> 的连接器<{new_name}>",
                old.name,
                old.id.value()
            );
            DbzConnectorUtils::deploy_connector(&new_url, &new_name, &config)
                .await
                .map_err(|e| anyhow!("下发上游连接器<{}>配置失败: {e}", old.name))?;
        }

        if let Some(enabled) = modify_dto.enabled.filter(|enabled| *enabled != old.enabled) {
            modify_dto.run_status = Some(if enabled {
                ConnectorRunStatus::None
            } else {
                ConnectorRunStatus::Disabled
            });
        } else if enabled_after && (config_changed || relocated) {
            // 连接器刚被注销或配置刚变更，实际状态要等下一轮检查确认
            modify_dto.run_status = Some(ConnectorRunStatus::None);
        }

        // 禁用不级联停下游，但「仍有下游在跑」必须说出来，否则用户会以为已经全停了
        if disabling {
            let query_dto = DbzSinkConnectorQueryDto::builder()
                .source_connector_id(old.id)
                .enabled(true)
                .build();
            let running_sinks =
                DbzSinkConnectorSvc::list_by_query_dto::<DatabaseConnection>(query_dto, None)
                    .await?
                    .extra
                    .unwrap_or_default()
                    .len();
            if running_sinks > 0 {
                warn!(
                    "上游连接器<{}>(id={})已被禁用，但其下仍有 {running_sinks} 个已启用的下游连接器，\
                     它们会继续被检查与自动修复；如需全部停止请分别禁用这些下游连接器",
                    old.name,
                    old.id.value()
                );
            }
        }

        Ok(modify_dto)
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

    /// # 定时检查所有连接器的运行状态
    ///
    /// 由 jobx 健康检查任务定时触发。检查范围是：
    /// * 已启用的上游连接器：探测 Kafka Connect 连通性 → 上游连接器 → 其下的
    ///   已启用下游连接器；
    /// * 未启用但名下仍有已启用下游连接器的上游：跳过它自己的连接器（禁用时已
    ///   注销），只检查其下游，避免这些下游的状态一直停在旧值；
    /// * 未启用且没有已启用下游的上游：完全不参与检查。
    ///
    /// 单个连接器检查失败不会中断整体遍历；上游 Kafka Connect 不可达时，上游与
    /// 其下所有下游都记为「连接 KafkaConnect 失败」。告警按根因收敛：上游启用时
    /// 只报一条上游告警（不再为每个下游重复报警）；上游已禁用时没有上游告警兜底，
    /// 改由各下游分别告警。
    ///
    /// 同一地址在本轮内只探测一次，且**只缓存成功**：缓存失败会把一次瞬时抖动
    /// 放大成该地址下所有连接器的「连接失败」，比重复探测更糟。
    ///
    /// ## 返回值
    /// * `Ok(String)` - 本次检查的执行摘要
    /// * `Err` - 基础设施错误（如数据库连接失败）
    pub async fn check_all_connectors() -> anyhow::Result<String> {
        info!("开始定时检查所有连接器的运行状态...");

        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;

        // 上游连接器是检查的入口（Kafka Connect 地址在它身上），不能只查已启用的：
        // 上游被禁用后，其下游仍可能单独保持启用并跑在 Kafka Connect 上。
        let source_ro = Self::list_by_query_dto::<DatabaseConnection>(
            DbzSourceConnectorQueryDto::builder().build(),
            Some(&db),
        )
        .await
        .map_err(|e| anyhow!("查询上游连接器列表失败: {e}"))?;
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
            "共找到 {} 个上游连接器、{} 个已启用的下游连接器，开始检查",
            sources.len(),
            sink_count
        );

        let mut errors: Vec<String> = Vec::new();
        // 本轮内已确认可达的 Kafka Connect 地址，避免多个上游引用同一台集群时重复探测
        let mut reachable_urls: HashSet<String> = HashSet::new();

        for source in &sources {
            let sinks = sinks_by_source
                .remove(&source.id.value())
                .unwrap_or_default();
            // 已禁用且没有启用中的下游，这条记录不用检查
            if !source.enabled && sinks.is_empty() {
                continue;
            }
            match Self::check_one_source(source, &sinks, &mut reachable_urls).await {
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
    /// 按以下顺序检查，各自回写状态：
    /// 1. Kafka Connect 连通性；不可达时上游与其下所有下游都记为连接失败并返回；
    /// 2. 上游连接器本身（上游已禁用时跳过，它的连接器已被注销）；
    /// 3. 其下每个已启用的下游连接器。
    ///
    /// ## 参数
    /// * `source` - 上游连接器
    /// * `sinks` - 挂在它下面的已启用下游连接器
    /// * `reachable_urls` - 本轮已确认可达的 Kafka Connect 地址
    ///
    /// ## 返回值
    /// * `Ok(Vec<String>)` - 异常连接器的详情列表；全部正常时为空
    /// * `Err(anyhow::Error)` - 基础设施错误（数据库读写失败）
    async fn check_one_source(
        source: &DbzSourceConnectorVo,
        sinks: &[DbzSinkConnectorVo],
        reachable_urls: &mut HashSet<String>,
    ) -> Result<Vec<String>, anyhow::Error> {
        let base_url = normalize_url(&source.kafka_connect_url);
        let source_label = format!("上游连接器<{}>(id={})", source.name, source.id.value());
        let mut errors: Vec<String> = Vec::new();

        // 1. 探测 Kafka Connect 连通性。地址不通时这台 Kafka Connect 上的连接器
        //    状态都查不到；下游与上游共用地址，同样判不了，所以一并记为连接失败，
        //    而不是把旧状态留在库里让人误以为还健康。
        //
        //    同一地址本轮已经探通过就直接复用；失败**不进**缓存，否则一次瞬时抖动
        //    会被放大成该地址下所有上游与下游的「连接失败」。
        let probe = if reachable_urls.contains(&base_url) {
            // 本轮已确认可达，直接复用探测结果
            Ok(())
        } else {
            DbzConnectorUtils::check_connect(&base_url).await
        };
        if let Err(e) = probe {
            warn!("{source_label}: Kafka Connect 连接失败: {e}");
            if source.enabled {
                Self::update_run_status(source, ConnectorRunStatus::KafkaConnectFailed).await?;
                Self::send_source_alarm(source, ConnectorRunStatus::KafkaConnectFailed).await;
                errors.push(format!(
                    "{source_label}: {}",
                    ConnectorRunStatus::KafkaConnectFailed.value()
                ));
            } else {
                // 上游已禁用，不会有上游告警兜底，这些下游的告警不能省
                for sink in sinks {
                    Self::send_sink_alarm(sink, source, ConnectorRunStatus::KafkaConnectFailed)
                        .await;
                }
            }
            // 上游启用时同一根因只报一条上游告警，不再逐个下游刷告警
            for sink in sinks {
                DbzSinkConnectorSvc::update_run_status(
                    sink,
                    ConnectorRunStatus::KafkaConnectFailed,
                )
                .await?;
                errors.push(format!(
                    "{}: {}",
                    Self::sink_label(sink, source),
                    ConnectorRunStatus::KafkaConnectFailed.value()
                ));
            }
            return Ok(errors);
        }
        // 探测成功才进缓存，失败留给下一个引用同一地址的上游重新探测
        reachable_urls.insert(base_url.clone());

        // 2. 上游连接器。已禁用时它的连接器已被注销，状态固定为「已禁用」，不再探测。
        if source.enabled {
            let check = ConnectorCheck {
                label: &source_label,
                base_url: &base_url,
                connector_name: &source.connector_name,
                connector_config: &source.connector_config,
            };
            match Self::ensure_and_check_connector(&check).await {
                Ok(_) => Self::update_run_status(source, ConnectorRunStatus::Running).await?,
                Err((run_status, action)) => {
                    Self::update_run_status(source, run_status).await?;
                    Self::send_source_alarm(source, run_status).await;
                    // 告警先发，再修复：自动修复不替代报警
                    Self::apply_repair(&base_url, &source.connector_name, action).await;
                    errors.push(format!("{source_label}: {}", run_status.value()));
                }
            }
        }

        // 3. 下游连接器
        for sink in sinks {
            let label = Self::sink_label(sink, source);
            let check = ConnectorCheck {
                label: &label,
                base_url: &base_url,
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
                    Self::send_sink_alarm(sink, source, run_status).await;
                    // 告警先发，再修复：自动修复不替代报警
                    Self::apply_repair(&base_url, &sink.connector_name, action).await;
                    errors.push(format!("{label}: {}", run_status.value()));
                }
            }
        }

        Ok(errors)
    }

    /// # 下游连接器的日志标签
    fn sink_label(sink: &DbzSinkConnectorVo, source: &DbzSourceConnectorVo) -> String {
        format!(
            "下游连接器<{}>(id={}, 上游<{}>)",
            sink.name,
            sink.id.value(),
            source.name
        )
    }

    /// # 上报上游连接器告警
    ///
    /// 告警是旁路动作：上报失败只记日志，不能中断检查流程（否则一台 alarm-svr
    /// 故障会让整轮检查提前结束、自动修复也不再执行）。
    async fn send_source_alarm(source: &DbzSourceConnectorVo, run_status: ConnectorRunStatus) {
        if let Err(e) = DbzAlarmUtils::send_source_connector_alarm(source, run_status).await {
            warn!("上游连接器<{}>告警上报失败: {e}", source.name);
        }
    }

    /// # 上报下游连接器告警
    ///
    /// 与 [`send_source_alarm`](Self::send_source_alarm) 一样，失败只记日志。
    async fn send_sink_alarm(
        sink: &DbzSinkConnectorVo,
        source: &DbzSourceConnectorVo,
        run_status: ConnectorRunStatus,
    ) {
        if let Err(e) = DbzAlarmUtils::send_sink_connector_alarm(sink, source, run_status).await {
            warn!("下游连接器<{}>告警上报失败: {e}", sink.name);
        }
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

        // 查询连接器状态（含任务状态）。刚部署的连接器会短暂处于 UNASSIGNED，
        // 这时判异常会误告警并触发无意义的重启，先等它过渡到实际状态。
        let (status, failed_task_ids) =
            Self::fetch_status(base_url, connector_name, label, !exists)
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
    ) -> Result<(ConnectorStatus, Vec<u32>), ConnectorError> {
        let mut result =
            DbzConnectorUtils::fetch_connector_status(base_url, connector_name).await?;
        if !just_deployed {
            return Ok(result);
        }

        for _ in 0..DEPLOY_STATUS_RETRIES {
            if result.0 != ConnectorStatus::Unassigned {
                break;
            }
            info!("{label}: 连接器<{connector_name}>刚部署，等待状态就绪...");
            tokio::time::sleep(Duration::from_millis(DEPLOY_STATUS_INTERVAL_MS)).await;
            result = DbzConnectorUtils::fetch_connector_status(base_url, connector_name).await?;
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
    /// 供健康检查回写检查结论使用。只带 `id` 与 `run_status` 调 `modify`，不会动
    /// 其它字段；没有 `enabled` / `kafka_connect_url` / `connector_name`，所以
    /// [`before_modify_hook`](Self::before_modify_hook) 会直接返回，不会触发注销。
    ///
    /// **状态没变就不写库**：检查每轮都在跑，状态常年是「运行中」，每轮写一次只会
    /// 让 `update_ms` 失去意义。不写之后 `update_ms` 表示的是「进入当前状态的时间」。
    /// 本轮的情况仍由返回的 `exec_detail` 与日志体现。
    async fn update_run_status(
        source: &DbzSourceConnectorVo,
        run_status: ConnectorRunStatus,
    ) -> Result<(), anyhow::Error> {
        if source.run_status == run_status {
            return Ok(());
        }

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
