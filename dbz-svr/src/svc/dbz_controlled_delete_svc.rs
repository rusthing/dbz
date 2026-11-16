//! # 受控删除（框架 CRUD delete 接口的审核口令闸门）
//!
//! 框架的 `#[router(crud)]` 会注册 `DELETE /dbz/{module}/{id}` 与
//! `DELETE /dbz/{module}`，它们直接调用 svc 的 `del_by_id` / `del_by_query_dto`，
//! 不经过控制面的「口令校验 + 审计」路径。这里把那道闸门抽成公共逻辑，让四个
//! CRUD 控制器在真正删除前先做同一件事：
//!
//! 1. 读 `Authorization` 与 `X-Audit-Pswd`，交给 adm-rs 校验当前用户的口令；
//! 2. 记下待删节点的名称（删完行就没了，审计只能靠删除前拿到的名字）；
//! 3. 删除成功后写一条不可变的 `dbz_delete_audit`。
//!
//! 口令校验失败时**不删除任何一行**，返回值仍是 HTTP 200 + `Ro::warn` + 业务码，
//! 与 `POST /dbz/control-plane/node-delete` 完全一致，前端可用同一套分流逻辑。

use crate::mo::dbz_delete_audit::{ActiveModel as AuditActiveModel, Entity as AuditEntity};
use crate::mo::{
    dbz_kafka_broker::Entity as BrokerEntity, dbz_kafka_connect::Entity as ConnectEntity,
    dbz_sink_connector::Entity as SinkEntity, dbz_source_connector::Entity as SourceEntity,
};
use crate::svc::{DbzDeleteVerifyError, DbzDeleteVerifySvc};
use crate::vo::DbzDeletePswdKind;
use anyhow::anyhow;
use robotech::api::U64;
use robotech::db::get_db_conn;
use robotech::svc::SvcError;
use sea_orm::{EntityTrait, Set};
use tracing::{info, warn};
use wheel_rs::time_utils::now_ms;

/// 审核口令摘要请求头
///
/// DELETE 不适合放请求体，口令摘要随 `Authorization` 一样走请求头，不落到 URL
/// 与访问日志里。
pub const AUDIT_PSWD_HEADER: &str = "x-audit-pswd";

/// # 画布节点类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbzNodeType {
    Broker,
    Connect,
    Source,
    Sink,
}

impl DbzNodeType {
    /// 审计表 `node_type` 列与前端约定使用的字符串
    pub fn code(self) -> &'static str {
        match self {
            Self::Broker => "broker",
            Self::Connect => "connect",
            Self::Source => "source",
            Self::Sink => "sink",
        }
    }

    /// 画布布局表 `node_type` 列的取值：1=Kafka 集群，2=Kafka Connect，3=上游，4=下游
    pub fn layout_code(self) -> i16 {
        match self {
            Self::Broker => 1,
            Self::Connect => 2,
            Self::Source => 3,
            Self::Sink => 4,
        }
    }

    /// 解析控制面传入的节点类型
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "broker" => Some(Self::Broker),
            "connect" => Some(Self::Connect),
            "source" => Some(Self::Source),
            "sink" => Some(Self::Sink),
            _ => None,
        }
    }
}

/// # 受控删除公共逻辑
pub struct DbzControlledDeleteSvc;

impl DbzControlledDeleteSvc {
    /// # 校验当前用户的审核口令
    ///
    /// `audit_pswd` 缺失按空口令处理（返回「口令不能为空」），不会因为没传头就放行。
    /// 成功时返回本次实际校验的口令类型，供调用方原样透传。
    pub async fn verify(
        authorization: Option<&str>,
        audit_pswd: Option<&str>,
    ) -> Result<DbzDeletePswdKind, DbzDeleteVerifyError> {
        DbzDeleteVerifySvc::verify(authorization, audit_pswd.unwrap_or("")).await
    }

    /// # 删除前读取节点名称
    ///
    /// 记录已不存在时返回 NotFound，与框架删除接口原本的语义一致。
    pub async fn name_of(node_type: DbzNodeType, node_id: i64) -> Result<String, SvcError> {
        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;
        let name = match node_type {
            DbzNodeType::Broker => BrokerEntity::find_by_id(node_id)
                .one(db.as_ref())
                .await
                .map_err(|e| anyhow!("查询 Kafka 集群失败: {e}"))?
                .map(|model| model.name),
            DbzNodeType::Connect => ConnectEntity::find_by_id(node_id)
                .one(db.as_ref())
                .await
                .map_err(|e| anyhow!("查询 Kafka Connect 失败: {e}"))?
                .map(|model| model.name),
            DbzNodeType::Source => SourceEntity::find_by_id(node_id)
                .one(db.as_ref())
                .await
                .map_err(|e| anyhow!("查询上游连接器失败: {e}"))?
                .map(|model| model.name),
            DbzNodeType::Sink => SinkEntity::find_by_id(node_id)
                .one(db.as_ref())
                .await
                .map_err(|e| anyhow!("查询下游连接器失败: {e}"))?
                .map(|model| model.name),
        };
        name.ok_or_else(|| SvcError::NotFound(node_id.to_string()))
    }

    /// # 写一条删除审计
    ///
    /// 审计失败不回滚删除：删除已经完成，这里只记错误日志，避免前端以为没删掉。
    /// `id` 由 idworker 生成——表里该列没有默认值，`ActiveModel` 不填就插不进去。
    pub async fn record(
        node_type: DbzNodeType,
        node_id: i64,
        node_name: &str,
        operator_id: U64,
    ) {
        let Ok(db) = get_db_conn() else {
            warn!(
                "写入删除审计失败（获取数据库连接失败）: node_type={}, node_id={}",
                node_type.code(),
                node_id
            );
            return;
        };
        let id = match idworker::next_id() {
            Ok(id) => id as i64,
            Err(error) => {
                warn!(
                    "写入删除审计失败（生成主键失败: {error}）: node_type={}, node_id={}",
                    node_type.code(),
                    node_id
                );
                return;
            }
        };
        let now = now_ms() as i64;
        let audit = AuditActiveModel {
            id: Set(id),
            node_type: Set(node_type.code().to_string()),
            node_id: Set(node_id),
            node_name: Set(node_name.to_string()),
            operator_id: Set(i64::from(operator_id)),
            delete_ms: Set(now),
            ..Default::default()
        };
        if let Err(error) = AuditEntity::insert(audit).exec(db.as_ref()).await {
            warn!("写入删除审计失败（删除已完成）: {error}");
        }
        info!(
            "删除审计: operator={}, node_type={}, node_id={}, node_name={}, delete_ms={}",
            operator_id.0,
            node_type.code(),
            node_id,
            node_name,
            now
        );
    }

    /// # 批量写删除审计（按条件删除时用）
    ///
    /// 名称必须在删除前从查询结果里取好，删完就查不到了。
    pub async fn record_many(
        node_type: DbzNodeType,
        items: &[(i64, String)],
        operator_id: U64,
    ) {
        for (node_id, node_name) in items {
            Self::record(node_type, *node_id, node_name, operator_id).await;
        }
    }
}
