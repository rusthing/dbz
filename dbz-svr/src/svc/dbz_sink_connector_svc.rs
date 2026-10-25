//! # 下游连接器服务
//!
//! 下游（sink）连接器把 Kafka 中的变更写入目标存储。它通过 `source_connector_id`
//! 归属于某个上游连接器（一条上游可挂多条下游），通过 `kafka_connect_id` 指定
//! 运行它的 Kafka Connect 集群。两个字段互相独立：改挂上游只是改变「这条下游同步
//! 的是哪条上游」，连接器在集群上的位置不变；换集群才会真正搬家。
//!
//! 下游连接器的检查由 [`DbzKafkaConnectSvc::check_all_connectors`] 统一驱动，
//! 本服务只负责自身记录的写操作与状态回写。写操作同样**当场**同步到 Kafka Connect：
//! 新增后立即部署、启用后立即部署、改配置或搬家先注销再下发、禁用与删除先注销；
//! 定时检查只兜底补部署。
//!
//! [`DbzKafkaConnectSvc::check_all_connectors`]: crate::svc::DbzKafkaConnectSvc::check_all_connectors

use crate::dic::ConnectorRunStatus;
use crate::svc::DbzKafkaConnectSvc;
use crate::utils::{KafkaConnectorUtils, normalize_url, should_deploy};
use anyhow::anyhow;
use robotech::macros::svc;
use sea_orm::DatabaseConnection;
use tracing::{info, warn};
use wheel_rs::time_utils::now_ms;

#[svc(
    before_add = DbzSinkConnectorSvc::before_add_hook,
    after_add = DbzSinkConnectorSvc::after_add_hook,
    before_modify = DbzSinkConnectorSvc::before_modify_hook,
    before_del_by_id = DbzSinkConnectorSvc::before_del_by_id_hook
)]
pub struct DbzSinkConnectorSvc;

// SvcError 由框架定义且体积较大，而钩子/服务方法的返回类型必须与宏约定一致，
// 无法改成 Box 包裹，这里统一放行该 lint
#[allow(clippy::result_large_err)]
impl DbzSinkConnectorSvc {
    /// # 新增前钩子：新建即禁用时直接落为「已禁用」
    ///
    /// 被禁用的记录不参与健康检查，不在这里写状态就会一直停在「无状态」。
    async fn before_add_hook(
        mut add_dto: DbzSinkConnectorAddDto,
    ) -> Result<DbzSinkConnectorAddDto, SvcError> {
        if add_dto.enabled == Some(false) {
            add_dto.run_status = Some(ConnectorRunStatus::Disabled);
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
    async fn after_add_hook(
        _add_dto: DbzSinkConnectorAddDto,
        one: DbzSinkConnectorVo,
    ) -> Result<DbzSinkConnectorVo, SvcError> {
        if !one.enabled {
            return Ok(one);
        }
        if let Err(error) = Self::deploy_now(&one).await {
            warn!(
                "下游连接器<{}>(id={}) 新增后立即下发失败: {error}",
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
                    "下游连接器<{}>(id={}) 下发失败的结论没能写回: {e}",
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
    async fn deploy_now(one: &DbzSinkConnectorVo) -> Result<(), anyhow::Error> {
        let url = DbzKafkaConnectSvc::get_url(one.kafka_connect_id).await?;
        info!(
            "下游连接器<{}>(id={}): 下发到 <{url}> 的连接器<{}>",
            one.name,
            one.id.value(),
            one.connector_name
        );
        KafkaConnectorUtils::deploy_connector(&url, &one.connector_name, &one.connector_config)
            .await
            .map_err(|e| anyhow!("下发下游连接器<{}>配置失败: {e}", one.name))
    }

    /// # 修改前钩子：换集群、改名、下发配置、启用部署、禁用注销与状态同步
    ///
    /// 由 `#[svc]` 宏在 `modify` 写库**之前**调用。下游记录一改动，Kafka Connect
    /// 上的连接器不会自动跟着变：
    ///
    /// * **换集群**：旧集群上的同名连接器仍在往目标端写；不注销的话，紧接着的下发
    ///   会在新集群再建一个，两份连接器同时消费，目标端要么重复写入，要么分区被
    ///   劈成两半。
    /// * **改名**：旧名字的连接器会变成不受管理的「幽灵连接器」，与改名后的新
    ///   连接器一起写同一份数据。
    /// * **改配置**：健康检查只在连接器「不存在」时才部署，`connector_config`
    ///   不主动下发就永远不会生效。这里按新配置 PUT 一次（`PUT
    ///   /connectors/{name}/config` 对已存在的是更新、对不存在的是创建），换位置
    ///   时也顺带在新位置建好，不必等下一轮检查。配置非法则请求失败，不会把一条
    ///   跑不起来的配置留在库里。
    /// * **启用**：禁用时连接器已被注销，重新启用要立刻再部署回去，同样不等下一轮
    ///   检查 —— 开关显示「已启用」而集群上还没有这个连接器，是最容易让人误判的状态。
    /// * **禁用**：必须先停连接器再改库。注销失败则整个修改请求失败 —— 宁可禁用
    ///   不成，也不允许出现「库里已禁用、连接器还在跑」。
    ///
    /// 改挂上游（`source_connector_id`）不在上面之列：它只改变这条下游归属于哪条
    /// 上游，连接器在集群上的位置没有变化，不需要注销重部署。
    ///
    /// 注销与下发都在写库前；若随后写库失败，记录仍指向旧位置，下一轮检查会把
    /// 连接器重新部署回去，可自愈。
    ///
    /// ## 返回值
    /// * `Ok(ModifyDto)` - 补好 `run_status` 的修改参数
    /// * `Err(SvcError)` - 查询原记录失败，或注销/下发配置失败
    async fn before_modify_hook(
        mut modify_dto: DbzSinkConnectorModifyDto,
    ) -> Result<DbzSinkConnectorModifyDto, SvcError> {
        let Some(id) = modify_dto.id else {
            return Ok(modify_dto);
        };
        // 健康检查只回写 run_status，下面的判断都用不上，跳过这次查询
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
        let config_changed = modify_dto
            .connector_config
            .as_deref()
            .is_some_and(|config| config != old.connector_config);
        let disabling = modify_dto.enabled == Some(false) && old.enabled;
        // 禁用时连接器已被注销，重新启用必须再部署回去 —— 否则要等下一轮检查，
        // 用户在开关上看到「已启用」，集群上却还没有这个连接器
        let enabling = modify_dto.enabled == Some(true) && !old.enabled;
        let enabled_after = modify_dto.enabled.unwrap_or(old.enabled);

        // 旧地址为空说明这台集群上从未部署过连接器，没有可注销的东西，直接放行
        if !old_url.is_empty() && (relocated || disabling) {
            info!(
                "下游连接器<{}>(id={}): 旧连接器<{}>在 <{old_url}> 上，先注销再写库",
                old.name,
                old.id.value(),
                old.connector_name
            );
            KafkaConnectorUtils::delete_connector(&old_url, &old.connector_name).await?;
        }

        // 配置改动、换位置或重新启用后主动下发一次，避免等下一轮检查；禁用状态下不发，
        // 否则会把连接器重新创建出来
        if should_deploy(enabled_after, config_changed, relocated, enabling) && !new_url.is_empty()
        {
            let config = modify_dto
                .connector_config
                .clone()
                .unwrap_or_else(|| old.connector_config.clone());
            info!(
                "下游连接器<{}>(id={}): 启用或位置/配置变更，下发到 <{new_url}> 的连接器<{new_name}>",
                old.name,
                old.id.value()
            );
            KafkaConnectorUtils::deploy_connector(&new_url, &new_name, &config)
                .await
                .map_err(|e| anyhow!("下发下游连接器<{}>配置失败: {e}", old.name))?;
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
            // 配置刚下发或连接器刚被注销，实际状态要等下一轮检查确认
            modify_dto.run_status = Some(ConnectorRunStatus::None);
            modify_dto.run_msg = Some(None);
        }

        Ok(modify_dto)
    }

    /// # 删除前钩子：先注销连接器再删记录
    ///
    /// 记录一删，这个连接器就再没有任何地方管理它 —— 健康检查找不到它，不会告警
    /// 也不会自动修复，它会一直往目标端写。所以必须先注销；注销失败则拒绝删除：
    /// 宁可这次删不掉，也不留一个无人管理的连接器。
    ///
    /// 集群地址取不到（记录缺失或地址为空）时跳过注销：说明它从未被部署过。
    ///
    /// ## 参数
    /// * `id` - 待删除的下游连接器ID
    ///
    /// ## 返回值
    /// * `Ok(U64)` - 连接器已注销（或本来就不存在），原样返回 ID
    /// * `Err(SvcError)` - 注销连接器失败
    async fn before_del_by_id_hook(id: U64) -> Result<U64, SvcError> {
        let Some(one) = Self::get_by_id::<DatabaseConnection>(id, None).await?.extra else {
            return Ok(id);
        };
        match DbzKafkaConnectSvc::get_url(one.kafka_connect_id).await {
            Ok(url) => {
                info!(
                    "下游连接器<{}>(id={}): 删除记录前从 <{url}> 注销连接器<{}>",
                    one.name,
                    one.id.value(),
                    one.connector_name
                );
                KafkaConnectorUtils::delete_connector(&url, &one.connector_name).await?;
            }
            Err(e) => warn!(
                "下游连接器<{}>(id={}): 取不到集群地址，跳过注销: {e}",
                one.name,
                one.id.value()
            ),
        }
        Ok(id)
    }

    /// # 更新下游连接器的运行状态与检查结论
    ///
    /// 供 [`DbzKafkaConnectSvc`] 在健康检查时复用。只带 `id`、`run_status` 与
    /// `run_msg`，不会动其它字段；没有 `enabled` / `kafka_connect_id` /
    /// `connector_name`，所以 [`before_modify_hook`](Self::before_modify_hook)
    /// 会直接返回，不会触发注销。
    ///
    /// **状态和结论都没变就不写库**：检查每轮都在跑，状态常年是「运行中」，每轮写
    /// 一次只会让 `update_ms` 失去意义。结论变了要写——失败原因从「连接被拒」变成
    /// 「认证失败」时，正是最该让人看到的时刻。
    ///
    /// ## 参数
    /// * `sink` - 下游连接器
    /// * `run_status` - 最新的运行状态
    /// * `run_msg` - 失败原因（正常时传 `None`，库里会被清空）
    pub(crate) async fn update_run_status(
        sink: &DbzSinkConnectorVo,
        run_status: ConnectorRunStatus,
        run_msg: Option<String>,
    ) -> Result<(), anyhow::Error> {
        if sink.run_status == run_status && sink.run_msg == run_msg {
            return Ok(());
        }

        let mut modify_dto = DbzSinkConnectorModifyDto::builder()
            .id(sink.id)
            .run_status(run_status)
            ._current_user_id(U64(1))
            ._current_ms(U64(now_ms()))
            .build();
        // 直接赋值：字段是 Option<Option<String>>，Some(None) 才是「置 NULL」
        modify_dto.run_msg = Some(run_msg);

        Self::modify::<DatabaseConnection>(modify_dto, None)
            .await
            .map_err(|e| anyhow!("更新下游连接器<{}>运行状态失败: {e}", sink.name))?;

        info!(
            "下游连接器<{}>运行状态已更新: {}",
            sink.name,
            run_status.value()
        );
        Ok(())
    }
}
