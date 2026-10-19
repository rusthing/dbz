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

#[svc(after_modify = DbzSinkConnectorSvc::after_modify_hook)]
pub struct DbzSinkConnectorSvc;

impl DbzSinkConnectorSvc {
    /// # 修改后钩子：下游连接器被禁用时注销连接器
    ///
    /// 由 `#[svc]` 宏在 `modify` 方法执行后自动调用。
    /// 若记录被禁用（`modify_dto.enabled = Some(false)`），按其上游连接器的
    /// Kafka Connect 地址注销该连接器。
    ///
    /// 注：连接器名称变更场景中旧名称无法在 after_modify 中检测（数据库已更新），
    /// 若需处理名称变更，应先禁用旧记录再创建新记录。
    async fn after_modify_hook(
        modify_dto: DbzSinkConnectorModifyDto,
        one: DbzSinkConnectorVo,
    ) -> Result<DbzSinkConnectorVo, SvcError> {
        if modify_dto.enabled == Some(false) {
            let base_url =
                DbzSourceConnectorSvc::kafka_connect_url(one.source_connector_id).await?;
            info!(
                "下游连接器<{}>(id={}): 被禁用，注销连接器<{}>",
                one.name, one.id, one.connector_name
            );
            DbzConnectorUtils::delete_connector(&base_url, &one.connector_name).await?;
        }

        Ok(one)
    }

    /// # 更新下游连接器的运行状态
    ///
    /// 通过 SVC 的 modify 方法将最新的运行状态回写到数据库。
    /// 供 [`DbzSourceConnectorSvc`] 在健康检查时复用。
    ///
    /// ## 参数
    /// * `sink` - 下游连接器
    /// * `run_status` - 最新的运行状态
    pub(crate) async fn update_run_status(
        sink: &DbzSinkConnectorVo,
        run_status: ConnectorRunStatus,
    ) -> Result<(), anyhow::Error> {
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
