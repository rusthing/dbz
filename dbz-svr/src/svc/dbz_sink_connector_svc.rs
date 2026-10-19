//! # 下游连接器服务
//!
//! 下游（sink）连接器把 Kafka 中的变更写入目标存储。它通过 `source_connector_id`
//! 归属于某个上游连接器（一条上游可挂多条下游），自身**不保存** Kafka Connect
//! 地址 —— 沿用上游连接器配置的地址，即两者必须注册在同一个 Kafka Connect 上。
//!
//! 下游连接器的检查由 [`DbzSourceConnectorSvc::check_all_connectors`] 统一驱动，
//! 本服务只负责自身记录的写操作与状态回写。
//!
//! [`DbzSourceConnectorSvc::check_all_connectors`]: crate::svc::DbzSourceConnectorSvc::check_all_connectors

use crate::dic::ConnectorRunStatus;
use crate::svc::DbzSourceConnectorSvc;
use crate::utils::DbzConnectorUtils;
use anyhow::anyhow;
use robotech::macros::svc;
use sea_orm::DatabaseConnection;
use tracing::info;
use wheel_rs::time_utils::now_ms;

#[svc(
    before_add = DbzSinkConnectorSvc::before_add_hook,
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

    /// # 修改前钩子：连接器搬家、下发配置、禁用注销与状态同步
    ///
    /// 由 `#[svc]` 宏在 `modify` 写库**之前**调用。下游不保存 Kafka Connect 地址，
    /// 地址始终按上游取；但记录一改动，Kafka Connect 上的连接器不会自动跟着变：
    ///
    /// * **改挂上游**：新上游在另一台 Kafka Connect 上时，旧集群上的同名连接器
    ///   仍在往目标端写；不注销的话，紧接着的下发会在新集群再建一个，两份连接器
    ///   同时消费，目标端要么重复写入，要么分区被劈成两半。
    /// * **改名**：旧名字的连接器会变成不受管理的「幽灵连接器」，与改名后的新
    ///   连接器一起写同一份数据。
    /// * **改配置**：健康检查只在连接器「不存在」时才部署，`connector_config`
    ///   不主动下发就永远不会生效。这里按新配置 PUT 一次（`PUT
    ///   /connectors/{name}/config` 对已存在的是更新、对不存在的是创建），搬家
    ///   时也顺带在新位置建好，不必等下一轮检查。配置非法则请求失败，不会把一条
    ///   跑不起来的配置留在库里。
    /// * **禁用**：必须先停连接器再改库。注销失败则整个修改请求失败 —— 宁可禁用
    ///   不成，也不允许出现「库里已禁用、连接器还在跑」。
    ///
    /// 注销与下发都在写库前；若随后写库失败，记录仍指向旧位置，下一轮检查会把
    /// 连接器重新部署回去，可自愈。
    ///
    /// 新旧上游在同一台 Kafka Connect 上、名称与配置都没变时什么都不用做：连接器
    /// 原地不动，检查仍能找到它。
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
        if modify_dto.source_connector_id.is_none()
            && modify_dto.enabled.is_none()
            && modify_dto.connector_name.is_none()
            && modify_dto.connector_config.is_none()
        {
            return Ok(modify_dto);
        }
        let Some(old) = Self::get_by_id::<DatabaseConnection>(id, None).await?.extra else {
            return Ok(modify_dto);
        };

        let source_changed = modify_dto
            .source_connector_id
            .is_some_and(|new_id| new_id != old.source_connector_id);
        let name_changed = modify_dto
            .connector_name
            .as_deref()
            .map(str::trim)
            .is_some_and(|name| name != old.connector_name);
        let config_changed = modify_dto
            .connector_config
            .as_deref()
            .is_some_and(|config| config != old.connector_config);
        let disabling = modify_dto.enabled == Some(false) && old.enabled;
        let enabled_after = modify_dto.enabled.unwrap_or(old.enabled);

        // 连接器是否换了位置（Kafka Connect 地址或连接器名变了）
        let mut moved = false;
        if source_changed || name_changed || disabling || (config_changed && enabled_after) {
            let old_url = DbzSourceConnectorSvc::kafka_connect_url(old.source_connector_id).await?;
            // 先取到新上游的地址，避免改挂完才发现新地址不可用
            let new_url = match modify_dto.source_connector_id {
                Some(new_id) if source_changed => {
                    DbzSourceConnectorSvc::kafka_connect_url(new_id).await?
                }
                _ => old_url.clone(),
            };
            moved = new_url != old_url || name_changed;

            if moved || disabling {
                info!(
                    "下游连接器<{}>(id={}): 旧连接器<{}>在 <{old_url}> 上，先注销再写库",
                    old.name,
                    old.id.value(),
                    old.connector_name
                );
                DbzConnectorUtils::delete_connector(&old_url, &old.connector_name).await?;
            }

            // 配置改动或搬家后主动下发一次，避免等下一轮检查；禁用状态下不发，
            // 否则会把连接器重新创建出来
            if enabled_after && (config_changed || moved) {
                let name = modify_dto
                    .connector_name
                    .as_deref()
                    .map(str::trim)
                    .unwrap_or(&old.connector_name)
                    .to_string();
                let config = modify_dto
                    .connector_config
                    .clone()
                    .unwrap_or_else(|| old.connector_config.clone());
                info!(
                    "下游连接器<{}>(id={}): 位置或配置已变更，下发到 <{new_url}> 的连接器<{name}>",
                    old.name,
                    old.id.value()
                );
                DbzConnectorUtils::deploy_connector(&new_url, &name, &config)
                    .await
                    .map_err(|e| anyhow!("下发下游连接器<{}>配置失败: {e}", old.name))?;
            }
        }

        if let Some(enabled) = modify_dto.enabled.filter(|enabled| *enabled != old.enabled) {
            modify_dto.run_status = Some(if enabled {
                ConnectorRunStatus::None
            } else {
                ConnectorRunStatus::Disabled
            });
        } else if enabled_after && (config_changed || moved) {
            // 配置刚下发或连接器刚被注销，实际状态要等下一轮检查确认
            modify_dto.run_status = Some(ConnectorRunStatus::None);
        }

        Ok(modify_dto)
    }

    /// # 删除前钩子：先注销连接器再删记录
    ///
    /// 记录一删，这个连接器就再没有任何地方管理它 —— 健康检查找不到它，不会告警
    /// 也不会自动修复，它会一直往目标端写。所以必须先注销；注销失败则拒绝删除：
    /// 宁可这次删不掉，也不留一个无人管理的连接器。
    ///
    /// 上游记录或地址取不到时跳过注销（没有地址就说明从未部署过）。
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
        let Some(source) =
            DbzSourceConnectorSvc::get_by_id::<DatabaseConnection>(one.source_connector_id, None)
                .await?
                .extra
        else {
            return Ok(id);
        };

        let url = source.kafka_connect_url.trim().trim_end_matches('/');
        if !url.is_empty() {
            info!(
                "下游连接器<{}>(id={}): 删除记录前从 <{url}> 注销连接器<{}>",
                one.name,
                one.id.value(),
                one.connector_name
            );
            DbzConnectorUtils::delete_connector(url, &one.connector_name).await?;
        }
        Ok(id)
    }

    /// # 更新下游连接器的运行状态
    ///
    /// 供 [`DbzSourceConnectorSvc`] 在健康检查时复用。只带 `id` 与 `run_status`，
    /// 不会动其它字段；没有 `enabled` / `source_connector_id` / `connector_name`，
    /// 所以 [`before_modify_hook`](Self::before_modify_hook) 会直接返回，不会触发注销。
    ///
    /// **状态没变就不写库**：检查每轮都在跑，状态常年是「运行中」，每轮写一次只会
    /// 让 `update_ms` 失去意义。不写之后 `update_ms` 表示的是「进入当前状态的时间」。
    /// 本轮的情况仍由返回的 `exec_detail` 与日志体现。
    ///
    /// ## 参数
    /// * `sink` - 下游连接器
    /// * `run_status` - 最新的运行状态
    pub(crate) async fn update_run_status(
        sink: &DbzSinkConnectorVo,
        run_status: ConnectorRunStatus,
    ) -> Result<(), anyhow::Error> {
        if sink.run_status == run_status {
            return Ok(());
        }

        let modify_dto = DbzSinkConnectorModifyDto::builder()
            .id(sink.id)
            .run_status(run_status)
            ._current_user_id(U64(1))
            ._current_ms(U64(now_ms()))
            .build();

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
