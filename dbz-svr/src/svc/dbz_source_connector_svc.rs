//! # 上游连接器服务
//!
//! 上游（source）连接器是 Debezium 从源库读取变更并写入 Kafka 的连接器；一条
//! `dbz_source_connector` 记录对应 Kafka Connect 上的一个连接器实例，通过
//! `kafka_connect_id` 指明它跑在哪台集群上。下游（sink）连接器通过
//! `source_connector_id` 归属于它，见 [`DbzSinkConnectorSvc`](crate::svc::DbzSinkConnectorSvc)。
//!
//! 本服务只负责自身记录的写操作，而且每个写操作都**当场**同步到 Kafka Connect：
//! 新增后立即部署、启用后立即部署、改配置/搬家先注销再下发、禁用与删除先注销。
//! 定时健康检查只做兜底（发现集群上没有才补一次部署），
//! 见 [`DbzKafkaConnectSvc::check_all_connectors`](crate::svc::DbzKafkaConnectSvc::check_all_connectors)。

use crate::dic::ConnectorRunStatus;
use crate::dto::DbzSinkConnectorQueryDto;
use crate::svc::{DbzKafkaBrokerSvc, DbzKafkaConnectSvc, DbzSinkConnectorSvc};
use crate::utils::{DbzSignalUtils, KafkaConnectorUtils, normalize_url, should_deploy};
use anyhow::anyhow;
use robotech::macros::svc;
use sea_orm::DatabaseConnection;
use tracing::{info, warn};
use wheel_rs::time_utils::now_ms;

#[svc(
    before_add = DbzSourceConnectorSvc::before_add_hook,
    after_add = DbzSourceConnectorSvc::after_add_hook,
    before_modify = DbzSourceConnectorSvc::before_modify_hook,
    before_del_by_id = DbzSourceConnectorSvc::before_del_by_id_hook
)]
pub struct DbzSourceConnectorSvc;

// SvcError 由框架定义且体积较大，而钩子/服务方法的返回类型必须与宏约定一致，
// 无法改成 Box 包裹，这里统一放行该 lint
#[allow(clippy::result_large_err)]
impl DbzSourceConnectorSvc {
    /// # 新增前钩子：新建即禁用时直接落为「已禁用」，并补上信号 broker 地址
    ///
    /// 被禁用的记录不参与健康检查，不在这里写状态就会一直停在「无状态」。
    ///
    /// 补地址见 [`fill_signal_bootstrap`](Self::fill_signal_bootstrap)：kafka 信号通道
    /// 的 broker 地址留空时连接器根本起不来，所以保存这一刻就填成关联集群的地址。
    async fn before_add_hook<C: ConnectionTrait>(
        mut add_dto: DbzSourceConnectorAddDto,
        _db: &C,
    ) -> Result<DbzSourceConnectorAddDto, SvcError> {
        if add_dto.enabled == Some(false) {
            add_dto.run_status = Some(ConnectorRunStatus::Disabled);
        }
        let filled = match (
            add_dto.connector_config.as_deref(),
            add_dto.kafka_connect_id,
        ) {
            (Some(config), Some(kafka_connect_id)) => {
                Self::fill_signal_bootstrap(config, kafka_connect_id).await?
            }
            _ => None,
        };
        if let Some(filled) = filled {
            add_dto.connector_config = Some(filled);
        }
        Ok(add_dto)
    }

    /// # 新增后钩子：立刻把连接器部署到集群
    ///
    /// 只存记录不动集群的话，用户点完「新增」看到的是「无状态」，要等最长一个
    /// 检查间隔才知道成没成、错在哪。这里当场下发一次（`PUT /connectors/{name}/config`
    /// 对不存在的是创建），把 Kafka Connect 的答复变成「注册失败 + 原因」写进记录，
    /// 列表里悬停就能看到。
    ///
    /// 部署失败**不用错误收场**：钩子跑在写库之后，此时报错只会让人以为没建成、
    /// 再点一次撞唯一约束。写回状态更诚实，而且下一轮检查发现集群上没有这个连接器
    /// 时会自己再部署一次。
    ///
    /// 禁用的记录不部署：启用时会部署（见 [`before_modify_hook`](Self::before_modify_hook)）。
    ///
    /// ## 返回值
    /// * `Ok(Vo)` - 原样返回新记录，不做修改
    async fn after_add_hook<C: ConnectionTrait>(
        _add_dto: DbzSourceConnectorAddDto,
        one: DbzSourceConnectorVo,
        _db: &C,
    ) -> Result<DbzSourceConnectorVo, SvcError> {
        if !one.enabled {
            return Ok(one);
        }
        if let Err(error) = Self::deploy_now(&one).await {
            warn!(
                "上游连接器<{}>(id={}) 新增后立即下发失败: {error}",
                one.name,
                one.id.value()
            );
            let reason = format!("{error:#}");
            if let Err(e) = Self::update_run_status(
                &one,
                ConnectorRunStatus::RegisterFailed,
                Some(reason.clone()),
            )
            .await
            {
                warn!(
                    "上游连接器<{}>(id={}) 下发失败的结论没能写回: {e}",
                    one.name,
                    one.id.value()
                );
            }
        }
        Ok(one)
    }

    /// # 把这条记录下发到它所在的集群
    ///
    /// 新增后钩子用：地址取不到、PUT 被拒都在这里变成人话。修改钩子不走它 ——
    /// 那边要下发的可能是「换位置后的新地址 + 新名字」，参数不一样。
    ///
    /// ## 参数
    /// * `one` - 连接器记录，用它的 `kafka_connect_id` / `connector_name` / `connector_config`
    ///
    /// ## 返回值
    /// * `Ok(())` - 已被集群接受
    /// * `Err(anyhow::Error)` - 集群地址缺失或集群拒绝该配置
    async fn deploy_now(one: &DbzSourceConnectorVo) -> Result<(), anyhow::Error> {
        let url = DbzKafkaConnectSvc::get_url(one.kafka_connect_id).await?;
        info!(
            "上游连接器<{}>(id={}): 下发到 <{url}> 的连接器<{}>",
            one.name,
            one.id.value(),
            one.connector_name
        );
        KafkaConnectorUtils::deploy_connector(&url, &one.connector_name, &one.connector_config)
            .await
            .map_err(|e| anyhow!("下发上游连接器<{}>配置失败: {e}", one.name))
    }

    /// # 补上 kafka 信号通道的 broker 地址
    ///
    /// 开了 kafka 通道却把 `signal.kafka.bootstrap.servers` 留空时，用这台连接器所在
    /// Kafka Connect 关联集群的地址填上去。这个键在 Debezium 里必填、没有默认值，
    /// 也不会沿用 worker 的地址：留空只会让连接器建 task 时抛 NPE，整条连接器起不来。
    ///
    /// 拿不到集群时**报错而不是放过**：放过的结果是保存成功、等健康检查才失败，而失败
    /// 原因是一条 Java 调用栈，看不出跟「没关联集群」有关。在这里报错能把话说清楚。
    ///
    /// ## 参数
    /// * `config` - 连接器配置 JSON
    /// * `kafka_connect_id` - 连接器所在的 Kafka Connect
    ///
    /// ## 返回值
    /// * `Ok(None)` - 不用改（没开 kafka 通道，或已经有地址）
    /// * `Ok(Some(String))` - 补好地址的配置
    /// * `Err(SvcError)` - 配置不合法，或取不到关联集群的地址
    async fn fill_signal_bootstrap(
        config: &str,
        kafka_connect_id: U64,
    ) -> Result<Option<String>, SvcError> {
        if !DbzSignalUtils::enabled_channels(config)
            .iter()
            .any(|channel| channel == "kafka")
        {
            return Ok(None);
        }

        let connect = DbzKafkaConnectSvc::get_by_id::<DatabaseConnection>(kafka_connect_id, None)
            .await?
            .extra
            .ok_or_else(|| anyhow!("Kafka Connect<{}>不存在", kafka_connect_id.value()))?;
        let broker_id = connect.kafka_broker_id;
        let broker = DbzKafkaBrokerSvc::load_conn(broker_id).await?;

        let filled = DbzSignalUtils::fill_kafka_bootstrap(config, &broker.bootstrap_servers)?;
        if filled.is_some() {
            info!(
                "连接器配置开了 kafka 信号通道但没有 broker 地址，用集群<{}>的地址 [{}] 补上",
                broker.name, broker.bootstrap_servers
            );
        }
        Ok(filled)
    }

    /// # 修改前钩子：换集群、改名、下发配置、启用部署、禁用注销与状态同步
    ///
    /// 由 `#[svc]` 宏在 `modify` 写库**之前**调用。记录上的改动必须同步到
    /// Kafka Connect，否则就只是改了库：
    ///
    /// 1. **换集群或改名**：Kafka Connect 上的连接器不会随记录迁移。旧位置上的
    ///    旧连接器仍在抓源库变更，而紧接着的下发会在新位置再建一个 —— 两个连接器
    ///    同时抓同一份变更，下游会收到两份。所以先按「旧地址 + 旧名字」注销，再写库。
    /// 2. **下发**：健康检查只在连接器「不存在」时才部署，改了 `connector_config`
    ///    若不主动下发就永远不会生效。这里按新配置 PUT 一次（`PUT
    ///    /connectors/{name}/config` 对已存在的是更新、对不存在的是创建），换位置
    ///    时也顺带在新位置建好，不必等下一轮检查。下发的是一份「补好 kafka 信号通道
    ///    broker 地址」的配置（见 [`fill_signal_bootstrap`](Self::fill_signal_bootstrap)），
    ///    配方里留空也部署得起来。配置非法则请求失败，不会把一条跑不起来的配置留在库里。
    /// 3. **启用**：禁用时连接器已被注销，重新启用要立刻再部署回去。同样不等下一轮
    ///    检查 —— 开关显示「已启用」而集群上还没有这个连接器，是最容易让人误判的状态。
    /// 4. **禁用**：先注销连接器再写库。注销失败则整个修改请求失败，宁可禁用不成，
    ///    也不允许出现「库里已禁用、连接器还在跑」。
    /// 5. **状态**：禁用置「已禁用」；重新启用、换位置或重新下发配置置「无状态」，
    ///    等下一轮检查判定。开关只有**真的变化**才改写 —— 普通编辑常会带上原有的
    ///    `enabled`，不能因此把检查得出的状态覆盖掉。
    ///
    /// 注销与下发都在写库前；若随后写库失败，记录仍指向旧位置，下一轮健康检查会
    /// 把连接器重新部署回去，可自愈。
    ///
    /// 注：不级联处理下游连接器 —— 它们各自有 `enabled` 与 `kafka_connect_id`；
    /// 上游被禁用后其下游仍会被检查，要彻底停止下游请单独禁用对应的下游连接器。
    ///
    /// ## 返回值
    /// * `Ok(ModifyDto)` - 补好 `run_status` 的修改参数
    /// * `Err(SvcError)` - 查询原记录失败，或注销/下发配置失败
    async fn before_modify_hook<C: ConnectionTrait>(
        mut modify_dto: DbzSourceConnectorModifyDto,
        _db: &C,
    ) -> Result<DbzSourceConnectorModifyDto, SvcError> {
        let Some(id) = modify_dto.id else {
            return Ok(modify_dto);
        };
        // 健康检查只回写 run_status，下面几个判断都用不上，跳过这次查询
        if modify_dto.enabled.is_none()
            && modify_dto.kafka_connect_id.is_none()
            && modify_dto.connector_name.is_none()
            && modify_dto.connector_config.is_none()
        {
            return Ok(modify_dto);
        }
        let Some(old) = Self::get_by_id::<DatabaseConnection>(id, None).await?.extra else {
            return Ok(modify_dto);
        };

        let old_url = DbzKafkaConnectSvc::get_url(old.kafka_connect_id).await?;
        let new_kafka_connect_id = modify_dto.kafka_connect_id.unwrap_or(old.kafka_connect_id);
        // 换集群时先取到新集群的地址，避免改完才发现新地址不可用
        let new_url = if new_kafka_connect_id != old.kafka_connect_id {
            DbzKafkaConnectSvc::get_url(new_kafka_connect_id).await?
        } else {
            old_url.clone()
        };
        let new_name = modify_dto
            .connector_name
            .as_deref()
            .map(str::trim)
            .unwrap_or(&old.connector_name)
            .to_string();
        let relocated =
            normalize_url(&new_url) != normalize_url(&old_url) || new_name != old.connector_name;
        // 最终要下发的配置：这次带了就用它，没带沿用库里那份
        let mut config_after = modify_dto
            .connector_config
            .clone()
            .unwrap_or_else(|| old.connector_config.clone());
        // 开了 kafka 信号通道却没填 broker 地址时补成关联集群的地址：这个键留空，
        // Debezium 建 task 时直接 NPE，整条连接器起不来。补出来的配置要写回库里，
        // 否则下次下发还是那份没有地址的。
        if let Some(filled) =
            Self::fill_signal_bootstrap(&config_after, new_kafka_connect_id).await?
        {
            config_after = filled;
            modify_dto.connector_config = Some(config_after.clone());
        }
        let config_changed = config_after != old.connector_config;
        let disabling = modify_dto.enabled == Some(false) && old.enabled;
        // 禁用时连接器已被注销，重新启用必须再部署回去 —— 否则要等下一轮检查，
        // 用户在开关上看到「已启用」，集群上却还没有这个连接器
        let enabling = modify_dto.enabled == Some(true) && !old.enabled;
        let enabled_after = modify_dto.enabled.unwrap_or(old.enabled);

        // 旧地址为空说明这台集群上从未部署过连接器，没有可注销的东西，直接放行
        if !old_url.is_empty() && (relocated || disabling) {
            info!(
                "上游连接器<{}>(id={}): 旧连接器<{}>在 <{old_url}> 上，先注销再写库",
                old.name,
                old.id.value(),
                old.connector_name
            );
            KafkaConnectorUtils::delete_connector(&old_url, &old.connector_name).await?;
        }

        // 配置改动、换位置或重新启用后主动下发一次，避免等下一轮检查（最长一个检查间隔）。
        // 禁用状态下不下发，否则会把连接器重新创建出来，与「已禁用」矛盾。
        if should_deploy(enabled_after, config_changed, relocated, enabling) && !new_url.is_empty()
        {
            info!(
                "上游连接器<{}>(id={}): 启用或位置/配置变更，下发到 <{new_url}> 的连接器<{new_name}>",
                old.name,
                old.id.value()
            );
            KafkaConnectorUtils::deploy_connector(&new_url, &new_name, &config_after)
                .await
                .map_err(|e| anyhow!("下发上游连接器<{}>配置失败: {e}", old.name))?;
        }

        if let Some(enabled) = modify_dto.enabled.filter(|enabled| *enabled != old.enabled) {
            modify_dto.run_status = Some(if enabled {
                ConnectorRunStatus::None
            } else {
                ConnectorRunStatus::Disabled
            });
            // 启停之后实际状态要等下一轮检查，上一轮的失败原因不再适用
            modify_dto.run_msg = Some(None);
        } else if enabled_after && (config_changed || relocated) {
            // 连接器刚被注销或配置刚变更，实际状态要等下一轮检查确认
            modify_dto.run_status = Some(ConnectorRunStatus::None);
            modify_dto.run_msg = Some(None);
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
    /// 集群地址取不到（记录缺失或地址为空）时跳过注销：说明它从未被部署过。
    ///
    /// ## 参数
    /// * `id` - 待删除的上游连接器ID
    ///
    /// ## 返回值
    /// * `Ok(U64)` - 连接器已注销（或本来就不存在），原样返回 ID
    /// * `Err(SvcError)` - 该上游下仍有下游连接器，或注销连接器失败
    async fn before_del_by_id_hook<C: ConnectionTrait>(id: U64, _db: &C) -> Result<U64, SvcError> {
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
        match DbzKafkaConnectSvc::get_url(one.kafka_connect_id).await {
            Ok(url) => {
                info!(
                    "上游连接器<{}>(id={}): 删除记录前从 <{url}> 注销连接器<{}>",
                    one.name,
                    one.id.value(),
                    one.connector_name
                );
                KafkaConnectorUtils::delete_connector(&url, &one.connector_name).await?;
            }
            Err(e) => warn!(
                "上游连接器<{}>(id={}): 取不到集群地址，跳过注销: {e}",
                one.name,
                one.id.value()
            ),
        }
        Ok(id)
    }

    /// # 更新上游连接器的运行状态与检查结论
    ///
    /// 供 [`DbzKafkaConnectSvc`] 在健康检查时复用。只带 `id`、`run_status` 与
    /// `run_msg` 调 `modify`，不会动其它字段；没有 `enabled` / `kafka_connect_id` /
    /// `connector_name`，所以 [`before_modify_hook`](Self::before_modify_hook)
    /// 会直接返回，不会触发注销。
    ///
    /// **状态和结论都没变就不写库**：检查每轮都在跑，状态常年是「运行中」，每轮写
    /// 一次只会让 `update_ms` 失去意义。结论变了要写——失败原因从「连接被拒」变成
    /// 「认证失败」时，正是最该让人看到的时刻。
    ///
    /// ## 参数
    /// * `source` - 库里的记录（用来比对是否真的有变化）
    /// * `run_status` - 检查得出的状态
    /// * `run_msg` - 失败原因（正常时传 `None`，库里会被清空）
    pub(crate) async fn update_run_status(
        source: &DbzSourceConnectorVo,
        run_status: ConnectorRunStatus,
        run_msg: Option<String>,
    ) -> Result<(), anyhow::Error> {
        if source.run_status == run_status && source.run_msg == run_msg {
            return Ok(());
        }

        let mut modify_dto = DbzSourceConnectorModifyDto::builder()
            .id(source.id)
            .run_status(run_status)
            ._current_user_id(U64(1))
            ._current_ms(U64(now_ms()))
            .build();
        // 直接赋值：字段是 Option<Option<String>>，Some(None) 才是「置 NULL」
        modify_dto.run_msg = Some(run_msg);

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
