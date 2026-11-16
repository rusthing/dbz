//! # 控制面节点删除服务
//!
//! 删除是高风险动作，必须先把当前登录用户的**高危操作口令**摘要连同登录令牌交给
//! adm-rs 校验（审核口令为空时由 adm-rs 回退登录口令），通过后才动数据库与远端
//! Connect；校验失败或认证服务不可用时**整笔操作都不执行**，不会留下「删除了一半」
//! 的状态。是否回退完全由 adm-rs 判定，本服务只透传结果里的口令类型。
//!
//! 删除成功后写一条不可变审计（操作人、时间、节点类型与名称），因为业务行删掉
//! 以后原来的 `updater_id` 也一起没了。口令校验与审计的公共逻辑见
//! [`DbzControlledDeleteSvc`](crate::svc::DbzControlledDeleteSvc)——框架 CRUD
//! 删除接口也走同一套。

use crate::dto::DbzControlPlaneNodeDeleteDto;
use crate::mo::dbz_control_plane_layout::{Column as LayoutColumn, Entity as LayoutEntity};
use crate::svc::{
    DbzControlledDeleteSvc, DbzKafkaBrokerSvc, DbzKafkaConnectSvc, DbzNodeType,
    DbzSinkConnectorSvc, DbzSourceConnectorSvc,
};
use crate::vo::DbzControlPlaneNodeDeleteVo;
use anyhow::anyhow;
use robotech::api::{Ro, U64};
use robotech::db::get_db_conn;
use robotech::svc::SvcError;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use tracing::warn;

/// # 控制面节点删除服务
pub struct DbzControlPlaneDeleteSvc;

impl DbzControlPlaneDeleteSvc {
    /// # 校验口令后删除一个节点
    ///
    /// * `authorization` - 原样转发给 adm-rs 的认证头（网关透传）
    pub async fn delete_node(
        dto: DbzControlPlaneNodeDeleteDto,
        operator_id: U64,
        authorization: Option<&str>,
    ) -> Result<Ro<DbzControlPlaneNodeDeleteVo>, SvcError> {
        let node = DbzNodeType::parse(dto.node_type.trim().to_lowercase().as_str())
            .ok_or_else(|| anyhow!("nodeType 只支持 broker / connect / source / sink"))?;
        let node_id = i64::from(dto.node_id);
        if node_id <= 0 {
            return Err(anyhow!("nodeId 必须为正数").into());
        }

        // 第一道门：口令校验。失败按业务码返回（HTTP 200 + warn），前端按码分流；
        // 后面的删除一行都不执行
        let pswd_kind = match DbzControlledDeleteSvc::verify(authorization, Some(&dto.audit_pswd))
            .await
        {
            Ok(kind) => kind,
            Err(failure) => {
                warn!(
                    "控制面删除口令校验未通过: code={}, msg={}",
                    failure.code, failure.msg
                );
                return Ok(Ro::warn(failure.msg).code(Some(failure.code)));
            }
        };

        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;
        let node_name = DbzControlledDeleteSvc::name_of(node, node_id).await?;

        match node {
            DbzNodeType::Broker => {
                DbzKafkaBrokerSvc::del_by_id::<sea_orm::DatabaseTransaction>(
                    U64::from(node_id),
                    None,
                )
                .await?;
            }
            DbzNodeType::Connect => {
                DbzKafkaConnectSvc::del_by_id::<sea_orm::DatabaseTransaction>(
                    U64::from(node_id),
                    None,
                )
                .await?;
            }
            DbzNodeType::Source => {
                DbzSourceConnectorSvc::del_by_id::<sea_orm::DatabaseTransaction>(
                    U64::from(node_id),
                    None,
                )
                .await?;
            }
            DbzNodeType::Sink => {
                DbzSinkConnectorSvc::del_by_id::<sea_orm::DatabaseTransaction>(
                    U64::from(node_id),
                    None,
                )
                .await?;
            }
        }

        // 业务行删除成功后再清布局，避免业务删除失败时先丢了位置
        let _ = LayoutEntity::delete_many()
            .filter(
                sea_orm::Condition::all()
                    .add(LayoutColumn::NodeType.eq(node.layout_code()))
                    .add(LayoutColumn::NodeId.eq(node_id)),
            )
            .exec(db.as_ref())
            .await;

        DbzControlledDeleteSvc::record(node, node_id, &node_name, operator_id).await;

        Ok(
            Ro::success(format!("已删除「{node_name}」"))
                .extra(Some(DbzControlPlaneNodeDeleteVo { pswd_kind })),
        )
    }
}
