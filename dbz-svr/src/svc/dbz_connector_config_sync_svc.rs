//! # 连接器配置同步服务
//!
//! 「不一致」对比弹窗里的双向同步：
//!
//! - `toRemote`：以库内配置为准，把（编辑后的）配置 `PUT` 到远端 Connect。
//!   会更新并重载远端连接器，风险与删除相当，因此要求当前登录用户的高危操作
//!   口令（审核口令为空时由 adm-rs 回退登录口令）；
//! - `toLocal`：以远端实际配置为准，把（编辑后的）配置写回库内纳管记录。
//!   只动库内数据，不要求口令。
//!
//! 同步完成后**重新计算** `inconsistent` 与脱敏快照：`toRemote` 以远端回读的
//! 实际配置为准，`toLocal` 以拉取到的远端配置为准，不会留下「同步成功但标记还在」
//! 的假标记。若用户编辑的字段与另一侧不同，差异会如实保留在标记里。

use crate::dao::{DbzSinkConnectorDao, DbzSourceConnectorDao};
use crate::dto::DbzConnectorConfigSyncDto;
use crate::mo::dbz_kafka_connect::Entity as ConnectEntity;
use crate::mo::dbz_sink_connector::ActiveModel as SinkActiveModel;
use crate::mo::dbz_sink_connector::Entity as SinkEntity;
use crate::mo::dbz_source_connector::ActiveModel as SourceActiveModel;
use crate::mo::dbz_source_connector::Entity as SourceEntity;
use crate::svc::DbzDeleteVerifySvc;
use crate::utils::{KafkaConnectorError, KafkaConnectorUtils, normalize_url};
use crate::vo::{DbzConnectorConfigSyncVo, DbzDeletePswdKind};
use anyhow::anyhow;
use robotech::api::{Ro, U64};
use robotech::db::get_db_conn;
use robotech::svc::SvcError;
use sea_orm::{ConnectionTrait, Set};
use serde_json::{Map, Value};
use tracing::{info, warn};
use wheel_rs::time_utils::now_ms;

use super::dbz_kafka_connect_svc::{compare_detail_text, config_mismatch, is_sensitive_key};

/// # 待同步的库内连接器
struct SyncTarget {
    id: i64,
    kafka_connect_id: i64,
    /// 记录名（提示文案用）
    name: String,
    /// Connect 上的连接器名
    connector_name: String,
    /// 库内配置 JSON 文本
    connector_config: String,
}

/// # 连接器配置同步服务
pub struct DbzConnectorConfigSyncSvc;

// SvcError 由框架定义且体积较大，这里统一放行与其它服务相同的 lint
#[allow(clippy::result_large_err)]
impl DbzConnectorConfigSyncSvc {
    /// # 同步一个连接器的配置
    ///
    /// * `authorization` - 原样转发给 adm-rs 的认证头（网关透传），仅 `toRemote` 使用
    pub async fn sync_config(
        dto: DbzConnectorConfigSyncDto,
        operator_id: U64,
        authorization: Option<&str>,
    ) -> Result<Ro<DbzConnectorConfigSyncVo>, SvcError> {
        let node_type = dto.node_type.trim().to_lowercase();
        if !matches!(node_type.as_str(), "source" | "sink") {
            return Err(anyhow!("nodeType 只支持 source / sink").into());
        }
        let direction = dto.direction.trim().to_lowercase();
        if !matches!(direction.as_str(), "toremote" | "tolocal") {
            return Err(anyhow!("direction 只支持 toRemote / toLocal").into());
        }
        let node_id = i64::from(dto.node_id);
        if node_id <= 0 {
            return Err(anyhow!("nodeId 必须为正数").into());
        }

        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;
        let target = Self::load_target(db.as_ref(), &node_type, node_id).await?;

        let connect = ConnectEntity::find_by_id(target.kafka_connect_id)
            .one(db.as_ref())
            .await
            .map_err(|e| anyhow!("查询 Kafka Connect 失败: {e}"))?
            .ok_or_else(|| anyhow!("连接器<{}>所属 Kafka Connect 不存在", target.name))?;
        if !connect.enabled {
            return Err(anyhow!(
                "所属 Kafka Connect<{}>已停用，无法与远端同步；请先启用后再试",
                connect.name
            )
            .into());
        }
        let base_url = normalize_url(&connect.url);
        if base_url.is_empty() {
            return Err(anyhow!("Kafka Connect<{}>未配置地址，无法同步", connect.name).into());
        }

        let patch = dto.patch.clone().unwrap_or_default();

        if direction == "toremote" {
            let pswd_kind = match Self::verify_password(
                authorization,
                dto.audit_pswd.as_deref().unwrap_or(""),
            )
            .await
            {
                Ok(kind) => kind,
                Err(ro) => return Ok(ro),
            };

            let local_value = Self::parse_object(&target.connector_config, "库内")?;
            let merged =
                Self::apply_patch(&local_value, &patch, &target.connector_name, "库内")?;
            let merged_text = serde_json::to_string(&merged)
                .map_err(|e| anyhow!("序列化连接器<{}>配置失败: {e}", target.name))?;

            KafkaConnectorUtils::deploy_connector(
                &base_url,
                &target.connector_name,
                &merged_text,
            )
            .await
            .map_err(|e| anyhow!("{}", Self::remote_error_text(e, "下发配置")))?;

            // 以远端回读结果为准重新判定，避免「已下发但标记还是旧值」
            let remote_after = KafkaConnectorUtils::fetch_connector_config(
                &base_url,
                &target.connector_name,
            )
            .await
            .map_err(|e| {
                anyhow!(
                    "配置已下发到远端，但回读远端配置失败：{}。库内记录未改动，请稍后点「检测」确认",
                    Self::remote_error_text(e, "读取配置")
                )
            })?;

            let mismatch = config_mismatch(&merged_text, &remote_after);
            let detail_text = compare_detail_text(
                &merged_text,
                &remote_after,
                mismatch,
                now_ms(),
                &target.name,
            )?;
            Self::save_target(
                db.as_ref(),
                &node_type,
                target.id,
                merged_text,
                mismatch,
                detail_text.clone(),
                i64::from(operator_id),
            )
            .await?;

            let changed = Self::changed_count(&detail_text);
            let msg = if mismatch {
                format!(
                    "已下发「{}」到远端；回读后仍有 {} 项差异，标记保留，可再次检测核对",
                    target.name, changed
                )
            } else {
                format!("已下发「{}」到远端，检测标记已重新计算为一致", target.name)
            };
            info!(
                "连接器<{}>(id={}) 配置已下发远端: operator={}, inconsistent={}, changed={}",
                target.name, target.id, operator_id.0, mismatch, changed
            );
            return Ok(Ro::success(msg).extra(Some(DbzConnectorConfigSyncVo {
                pswd_kind: Some(pswd_kind),
                inconsistent: mismatch,
                inconsistent_detail: detail_text,
            })));
        }

        // toLocal：远端配置是来源；远端不存在时没有可拉取的配置
        let remote =
            match KafkaConnectorUtils::fetch_connector_config(&base_url, &target.connector_name)
                .await
            {
                Ok(value) => value,
                Err(KafkaConnectorError::NotFound) => {
                    return Err(anyhow!(
                        "远端 Connect 上已不存在连接器<{}>，没有可拉取的配置；如需恢复，请改用「以库内为准」重新下发",
                        target.name
                    )
                    .into())
                }
                Err(e) => {
                    return Err(
                        anyhow!("{}", Self::remote_error_text(e, "读取配置")).into()
                    )
                }
            };

        let merged = Self::apply_patch(&remote, &patch, &target.connector_name, "远端")?;
        let merged_text = serde_json::to_string(&merged)
            .map_err(|e| anyhow!("序列化连接器<{}>配置失败: {e}", target.name))?;

        // 远端配置是基准；只有本次编辑过的字段才可能留下差异
        let mismatch = config_mismatch(&merged_text, &remote);
        let detail_text =
            compare_detail_text(&merged_text, &remote, mismatch, now_ms(), &target.name)?;
        Self::save_target(
            db.as_ref(),
            &node_type,
            target.id,
            merged_text,
            mismatch,
            detail_text.clone(),
            i64::from(operator_id),
        )
        .await?;

        let changed = Self::changed_count(&detail_text);
        let msg = if mismatch {
            format!(
                "已用远端配置覆盖库内「{}」；其中 {} 项是本次编辑、与远端不同，标记保留",
                target.name, changed
            )
        } else {
            format!("已用远端配置覆盖库内「{}」，检测标记已清除", target.name)
        };
        info!(
            "连接器<{}>(id={}) 已用远端配置写回库内: operator={}, inconsistent={}, changed={}",
            target.name, target.id, operator_id.0, mismatch, changed
        );
        Ok(Ro::success(msg).extra(Some(DbzConnectorConfigSyncVo {
            pswd_kind: None,
            inconsistent: mismatch,
            inconsistent_detail: detail_text,
        })))
    }

    /// 带「同步」动作名的口令校验；失败按业务码返回（HTTP 200 + warn）
    async fn verify_password(
        authorization: Option<&str>,
        audit_pswd: &str,
    ) -> Result<DbzDeletePswdKind, Ro<DbzConnectorConfigSyncVo>> {
        match DbzDeleteVerifySvc::verify_for(authorization, audit_pswd, "同步").await {
            Ok(kind) => Ok(kind),
            Err(failure) => {
                warn!(
                    "连接器配置同步口令校验未通过: code={}, msg={}",
                    failure.code, failure.msg
                );
                Err(Ro::warn(failure.msg).code(Some(failure.code)))
            }
        }
    }

    /// 读取库内连接器记录
    async fn load_target<C: ConnectionTrait>(
        db: &C,
        node_type: &str,
        node_id: i64,
    ) -> Result<SyncTarget, SvcError> {
        match node_type {
            "source" => {
                let model = SourceEntity::find_by_id(node_id)
                    .one(db)
                    .await
                    .map_err(|e| anyhow!("查询上游连接器失败: {e}"))?
                    .ok_or_else(|| anyhow!("上游连接器<{node_id}>不存在"))?;
                Ok(SyncTarget {
                    id: model.id,
                    kafka_connect_id: model.kafka_connect_id,
                    name: model.name,
                    connector_name: model.connector_name,
                    connector_config: model.connector_config,
                })
            }
            _ => {
                let model = SinkEntity::find_by_id(node_id)
                    .one(db)
                    .await
                    .map_err(|e| anyhow!("查询下游连接器失败: {e}"))?
                    .ok_or_else(|| anyhow!("下游连接器<{node_id}>不存在"))?;
                Ok(SyncTarget {
                    id: model.id,
                    kafka_connect_id: model.kafka_connect_id,
                    name: model.name,
                    connector_name: model.connector_name,
                    connector_config: model.connector_config,
                })
            }
        }
    }

    /// 写回配置并更新检测标记与快照（`inconsistent_detail` 为 `None` 时清空）
    async fn save_target<C: ConnectionTrait>(
        db: &C,
        node_type: &str,
        id: i64,
        config_text: String,
        mismatch: bool,
        detail_text: Option<String>,
        operator_id: i64,
    ) -> Result<(), SvcError> {
        match node_type {
            "source" => {
                let mut active = SourceActiveModel {
                    id: Set(id),
                    ..Default::default()
                };
                active.connector_config = Set(config_text);
                active.inconsistent = Set(mismatch);
                active.inconsistent_detail = Set(detail_text);
                active.updater_id = Set(operator_id);
                DbzSourceConnectorDao::update(active, db)
                    .await
                    .map_err(|e| anyhow!("回写上游连接器配置失败: {e}"))?;
            }
            _ => {
                let mut active = SinkActiveModel {
                    id: Set(id),
                    ..Default::default()
                };
                active.connector_config = Set(config_text);
                active.inconsistent = Set(mismatch);
                active.inconsistent_detail = Set(detail_text);
                active.updater_id = Set(operator_id);
                DbzSinkConnectorDao::update(active, db)
                    .await
                    .map_err(|e| anyhow!("回写下游连接器配置失败: {e}"))?;
            }
        }
        Ok(())
    }

    /// 解析配置文本为 JSON 对象
    fn parse_object(text: &str, side: &str) -> Result<Value, SvcError> {
        let value: Value = serde_json::from_str(text)
            .map_err(|e| anyhow!("{side}配置不是合法 JSON：{e}；请先修正后再同步"))?;
        if !value.is_object() {
            return Err(anyhow!("{side}配置不是 JSON 对象，无法同步").into());
        }
        Ok(value)
    }

    /// 在基准配置上应用用户改动
    ///
    /// - 只接受基准里已有的键，避免拼写错误悄悄写进一个无效配置；
    /// - `name` 由连接器名固定，不允许通过配置同步改名；
    /// - 敏感项留空（或仍是脱敏占位）视为「未重新输入」，保持基准里的真实值，
    ///   不会被占位符覆盖。
    fn apply_patch(
        base: &Value,
        patch: &Map<String, Value>,
        connector_name: &str,
        side: &str,
    ) -> Result<Value, SvcError> {
        let object = base
            .as_object()
            .ok_or_else(|| anyhow!("{side}配置不是 JSON 对象，无法同步"))?;
        let mut merged = object.clone();

        for (raw_key, value) in patch {
            let key = raw_key.trim();
            if key.is_empty() || key == "name" {
                continue;
            }
            if !object.contains_key(key) {
                return Err(anyhow!("配置项<{key}>不存在于{side}配置中，无法编辑").into());
            }
            if is_sensitive_key(key) {
                let text = value.as_str().map(str::trim).unwrap_or("");
                if text.is_empty() || text.contains("已脱敏") {
                    continue;
                }
            }
            merged.insert(key.to_string(), value.clone());
        }

        // PUT /connectors/{name}/config 要求配置里的 name 与 URL 上的名字一致
        merged.insert("name".to_string(), Value::String(connector_name.to_string()));
        Ok(Value::Object(merged))
    }

    /// 快照里的差异项数量
    fn changed_count(detail_text: &Option<String>) -> usize {
        detail_text
            .as_deref()
            .and_then(|text| serde_json::from_str::<Value>(text).ok())
            .and_then(|detail| {
                detail
                    .get("rows")
                    .and_then(Value::as_array)
                    .map(|rows| {
                        rows.iter()
                            .filter(|row| {
                                row.get("changed")
                                    .and_then(Value::as_bool)
                                    .unwrap_or(false)
                            })
                            .count()
                    })
            })
            .unwrap_or(0)
    }

    /// 把远端调用失败翻译成可定位的结果（拒绝 / 超时 / 连接器运行中 / 不存在）
    fn remote_error_text(error: KafkaConnectorError, action: &str) -> String {
        match error {
            KafkaConnectorError::NotFound => {
                format!("远端 Connect 上不存在该连接器，{action}失败")
            }
            KafkaConnectorError::Parse(msg) => {
                format!("远端 Connect 返回的配置无法解析（{action}）：{msg}")
            }
            KafkaConnectorError::Request(msg) => {
                let lower = msg.to_ascii_lowercase();
                if lower.contains("timed out") || lower.contains("timeout") {
                    format!("调用远端 Connect 超时（{action}）：{msg}")
                } else if lower.contains("409")
                    || lower.contains("running")
                    || lower.contains("conflict")
                {
                    format!(
                        "远端 Connect 拒绝了本次{action}（连接器可能正在运行或配置冲突）：{msg}"
                    )
                } else {
                    format!("远端 Connect {action}失败：{msg}")
                }
            }
        }
    }
}
