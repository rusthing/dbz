//! # 控制面节点删除服务
//!
//! 删除是高风险动作，必须先把当前登录用户的口令摘要连同登录令牌交给 adm-rs
//! 校验，通过后才动数据库与远端 Connect；校验失败或认证服务不可用时**整笔操作
//! 都不执行**，不会留下「删除了一半」的状态。
//!
//! 删除成功后写一条不可变审计（操作人、时间、节点类型与名称），因为业务行删掉
//! 以后原来的 `updater_id` 也一起没了。

use crate::dto::DbzControlPlaneNodeDeleteDto;
use crate::mo::dbz_control_plane_layout::{Column as LayoutColumn, Entity as LayoutEntity};
use crate::mo::dbz_delete_audit::{ActiveModel as AuditActiveModel, Entity as AuditEntity};
use crate::mo::{
    dbz_kafka_broker::Entity as BrokerEntity, dbz_kafka_connect::Entity as ConnectEntity,
};
use crate::mo::{
    dbz_sink_connector::Entity as SinkEntity, dbz_source_connector::Entity as SourceEntity,
};
use crate::svc::{
    DbzDeleteVerifySvc, DbzKafkaBrokerSvc, DbzKafkaConnectSvc, DbzSinkConnectorSvc,
    DbzSourceConnectorSvc,
};
use anyhow::anyhow;
use robotech::api::{Ro, U64};
use robotech::db::get_db_conn;
use robotech::svc::SvcError;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter};
use tracing::{info, warn};
use wheel_rs::time_utils::now_ms;

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
    ) -> Result<Ro<()>, SvcError> {
        let node_type = dto.node_type.trim().to_lowercase();
        if !matches!(node_type.as_str(), "broker" | "connect" | "source" | "sink") {
            return Err(anyhow!("nodeType 只支持 broker / connect / source / sink").into());
        }
        let node_id = i64::from(dto.node_id);
        if node_id <= 0 {
            return Err(anyhow!("nodeId 必须为正数").into());
        }

        // 第一道门：口令。失败直接返回，后面的删除一行都不执行
        DbzDeleteVerifySvc::verify(authorization, &dto.login_pswd).await?;

        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;
        let node_name = Self::name_of(db.as_ref(), &node_type, node_id).await?;

        match node_type.as_str() {
            "broker" => {
                DbzKafkaBrokerSvc::del_by_id::<sea_orm::DatabaseTransaction>(
                    U64::from(node_id),
                    None,
                )
                .await?;
            }
            "connect" => {
                DbzKafkaConnectSvc::del_by_id::<sea_orm::DatabaseTransaction>(
                    U64::from(node_id),
                    None,
                )
                .await?;
            }
            "source" => {
                DbzSourceConnectorSvc::del_by_id::<sea_orm::DatabaseTransaction>(
                    U64::from(node_id),
                    None,
                )
                .await?;
            }
            _ => {
                DbzSinkConnectorSvc::del_by_id::<sea_orm::DatabaseTransaction>(
                    U64::from(node_id),
                    None,
                )
                .await?;
            }
        }

        // 业务行删除成功后再清布局，避免业务删除失败时先丢了位置
        let node_type_code = match node_type.as_str() {
            "broker" => 1,
            "connect" => 2,
            "source" => 3,
            _ => 4,
        };
        let _ = LayoutEntity::delete_many()
            .filter(
                sea_orm::Condition::all()
                    .add(LayoutColumn::NodeType.eq(node_type_code))
                    .add(LayoutColumn::NodeId.eq(node_id)),
            )
            .exec(db.as_ref())
            .await;

        // 审计失败不回滚删除：删除已经完成，这里只记错误日志，避免前端以为没删掉
        let now = now_ms() as i64;
        let audit = AuditActiveModel {
            node_type: sea_orm::Set(node_type.clone()),
            node_id: sea_orm::Set(node_id),
            node_name: sea_orm::Set(node_name.clone()),
            operator_id: sea_orm::Set(i64::from(operator_id)),
            delete_ms: sea_orm::Set(now),
            ..Default::default()
        };
        if let Err(error) = AuditEntity::insert(audit).exec(db.as_ref()).await {
            warn!("写入删除审计失败（删除已完成）: {error}");
        }
        info!(
            "控制面删除审计: operator={}, node_type={}, node_id={}, node_name={}, delete_ms={}",
            operator_id.0, node_type, node_id, node_name, now
        );

        Ok(Ro::success(format!("已删除「{node_name}」")))
    }

    /// 删除前取到节点名称，用于审计与提示；记录已不存在时返回 NotFound
    async fn name_of<C: ConnectionTrait>(
        db: &C,
        node_type: &str,
        node_id: i64,
    ) -> Result<String, SvcError> {
        let name = match node_type {
            "broker" => BrokerEntity::find_by_id(node_id)
                .one(db)
                .await
                .map_err(|e| anyhow!("查询 Kafka 集群失败: {e}"))?
                .map(|model| model.name),
            "connect" => ConnectEntity::find_by_id(node_id)
                .one(db)
                .await
                .map_err(|e| anyhow!("查询 Kafka Connect 失败: {e}"))?
                .map(|model| model.name),
            "source" => SourceEntity::find_by_id(node_id)
                .one(db)
                .await
                .map_err(|e| anyhow!("查询上游连接器失败: {e}"))?
                .map(|model| model.name),
            _ => SinkEntity::find_by_id(node_id)
                .one(db)
                .await
                .map_err(|e| anyhow!("查询下游连接器失败: {e}"))?
                .map(|model| model.name),
        };
        name.ok_or_else(|| SvcError::NotFound(node_id.to_string()))
    }
}
