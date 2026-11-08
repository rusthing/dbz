//! # Kafka 集群备份 / 恢复服务
//!
//! ## 备份
//!
//! [`export`](DbzBackupSvc::export) 以整个 Kafka 集群为单位导出：集群本身（含
//! `sasl_password`）、其下全部 Kafka Connect、以及全部上游/下游连接器。产物只是一段
//! JSON，由前端下载保存，服务端不留档。
//!
//! ## 恢复
//!
//! [`restore`](DbzBackupSvc::restore) **只写库内纳管记录**：不向 Kafka Connect 注册
//! 连接器、也不删除任何远端连接器。是否下发由用户后续的启用/检测动作决定，避免
//! 「恢复一份备份」顺带改动远端集群。
//!
//! 恢复的冲突口径与 Connect 导入保持一致：
//!
//! * 连接器按 `connectorName` 判定是否为同一条记录；
//! * `skip` 只补齐缺失，`overwrite` 覆盖已存在记录；
//! * 下游按文件里的 `upstream.connectorName` 重新挂载，找不到上游就失败并说明，
//!   不静默丢弃。
//!
//! 同一个入口同时承担预览与执行：`dryRun=true` 只返回计划，`dryRun=false` 按同一套
//! 判定真实写库。页面预览看到的结论与执行结果因此不会出现两套逻辑。

use crate::dao::{
    DbzKafkaBrokerDao, DbzKafkaConnectDao, DbzSinkConnectorDao, DbzSourceConnectorDao,
};
use crate::dic::{ConnectorRunStatus, KafkaBrokerStatus, KafkaConnectStatus};
use crate::dto::{DbzBackupExportDto, DbzBackupRestoreDto};
use crate::mo::dbz_kafka_broker::{ActiveModel as BrokerActiveModel, Model as BrokerModel};
use crate::mo::dbz_kafka_connect::{ActiveModel as ConnectActiveModel, Model as ConnectModel};
use crate::mo::dbz_sink_connector::{ActiveModel as SinkActiveModel, Model as SinkModel};
use crate::mo::dbz_source_connector::{ActiveModel as SourceActiveModel, Model as SourceModel};
use crate::utils::normalize_url;
use crate::vo::{
    BACKUP_FORMAT, BACKUP_VERSION, DbzBackupBrokerVo, DbzBackupConnectVo, DbzBackupConnectorVo,
    DbzBackupFileVo, DbzBackupRestoreItemVo, DbzBackupRestoreVo, DbzBackupUpstreamVo,
};
use anyhow::anyhow;
use robotech::api::Ro;
use robotech::db::get_db_conn;
use sea_orm::{Condition, DatabaseConnection, Set};
use serde_json::{Map, Value};
use std::collections::HashMap;
use tracing::info;

/// 凭据字段：键名命中即视为敏感，`sasl.jaas.config` 整段值也含口令
const SENSITIVE_KEY_PATTERN: [&str; 4] = ["password", "secret", "token", "credential"];

/// # 备份 / 恢复服务
///
/// 没有对应的库表，因此不使用 `#[svc]` 宏，只提供两个关联方法。
pub struct DbzBackupSvc;

impl DbzBackupSvc {
    /// # 导出整个 Kafka 集群
    ///
    /// 集群本身按实体读取，因为 `DbzKafkaBrokerVo` 有意不返回密码；备份不导出密码就
    /// 恢复不出可用的连接，所以这里直接读 `mo::dbz_kafka_broker::Model`。
    ///
    /// ## 参数
    /// * `dto` - Kafka 集群 ID
    ///
    /// ## 返回值
    /// * `Ok(Ro<DbzBackupFileVo>)` - 可直接下载的备份文件内容
    /// * `Err(anyhow::Error)` - 集群不存在或读库失败
    pub async fn export(dto: DbzBackupExportDto) -> Result<Ro<DbzBackupFileVo>, anyhow::Error> {
        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;

        let broker = DbzKafkaBrokerDao::get_by_id::<_, BrokerModel>(dto.kafka_broker_id, db.as_ref())
            .await
            .map_err(|e| anyhow!("查询 Kafka 集群({})失败: {e}", dto.kafka_broker_id.value()))?
            .ok_or_else(|| anyhow!("Kafka 集群({})不存在", dto.kafka_broker_id.value()))?;

        let connects = DbzKafkaConnectDao::list_by_condition::<_, ConnectModel>(
            Condition::all(),
            &None,
            db.as_ref(),
        )
        .await
        .map_err(|e| anyhow!("查询 Kafka Connect 列表失败: {e}"))?;
        let sources = DbzSourceConnectorDao::list_by_condition::<_, SourceModel>(
            Condition::all(),
            &None,
            db.as_ref(),
        )
        .await
        .map_err(|e| anyhow!("查询上游连接器列表失败: {e}"))?;
        let sinks = DbzSinkConnectorDao::list_by_condition::<_, SinkModel>(
            Condition::all(),
            &None,
            db.as_ref(),
        )
        .await
        .map_err(|e| anyhow!("查询下游连接器列表失败: {e}"))?;

        let mut broker_connects: Vec<&ConnectModel> = connects
            .iter()
            .filter(|item| item.kafka_broker_id == broker.id)
            .collect();
        broker_connects.sort_by(|left, right| left.name.cmp(&right.name));

        let source_by_id: HashMap<i64, &SourceModel> =
            sources.iter().map(|item| (item.id, item)).collect();
        let connect_rows = broker_connects
            .into_iter()
            .map(|connect| {
                let mut rows: Vec<DbzBackupConnectorVo> = sources
                    .iter()
                    .filter(|item| item.kafka_connect_id == connect.id)
                    .map(|item| {
                        exported_connector(
                            "source",
                            &item.name,
                            &item.connector_name,
                            &item.connector_config,
                            item.remark.as_deref(),
                            item.enabled,
                            None,
                        )
                    })
                    .collect();
                rows.extend(
                    sinks
                        .iter()
                        .filter(|item| item.kafka_connect_id == connect.id)
                        .map(|item| {
                            let upstream = source_by_id.get(&item.source_connector_id).map(|up| {
                                DbzBackupUpstreamVo {
                                    id: Some(up.id.to_string()),
                                    name: up.name.clone(),
                                    connector_name: up.connector_name.clone(),
                                }
                            });
                            exported_connector(
                                "sink",
                                &item.name,
                                &item.connector_name,
                                &item.connector_config,
                                item.remark.as_deref(),
                                item.enabled,
                                upstream,
                            )
                        }),
                );
                rows.sort_by(|left, right| {
                    let order = connector_order(&left.direction).cmp(&connector_order(&right.direction));
                    order.then_with(|| left.name.cmp(&right.name))
                });
                DbzBackupConnectVo {
                    name: connect.name.clone(),
                    url: connect.url.clone(),
                    remark: connect.remark.clone(),
                    enabled: connect.enabled,
                    connectors: rows,
                }
            })
            .collect();

        let file = DbzBackupFileVo {
            format: BACKUP_FORMAT.to_string(),
            version: BACKUP_VERSION,
            exported_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            credential_mode: "included".to_string(),
            broker: DbzBackupBrokerVo {
                name: broker.name.clone(),
                bootstrap_servers: broker.bootstrap_servers.clone(),
                security_protocol: broker.security_protocol,
                sasl_mechanism: broker.sasl_mechanism,
                sasl_username: broker.sasl_username.clone(),
                sasl_password: broker.sasl_password.clone(),
                remark: broker.remark.clone(),
                enabled: broker.enabled,
            },
            connects: connect_rows,
        };

        info!(
            "导出 Kafka 集群<{}>(id={})备份：{} 台 Connect，{} 个连接器",
            broker.name,
            broker.id,
            file.connects.len(),
            file.connects.iter().map(|item| item.connectors.len()).sum::<usize>()
        );
        Ok(Ro::success(format!(
            "已导出 Kafka 集群「{}」：{} 台 Connect，{} 个连接器",
            file.broker.name,
            file.connects.len(),
            file.connects.iter().map(|item| item.connectors.len()).sum::<usize>()
        ))
        .extra(Some(file)))
    }

    /// # 恢复 Kafka 集群备份
    ///
    /// 文件校验不通过时仍返回成功，用 `valid=false` 与 `errors` 逐条说明原因 ——
    /// 页面据此展示与 Connect 导入一致的错误列表，而不是一个笼统的接口错误。
    ///
    /// ## 参数
    /// * `dto` - 备份文件、策略（`skip` / `overwrite`）与是否只预览
    ///
    /// ## 返回值
    /// * `Ok(Ro<DbzBackupRestoreVo>)` - 校验结论、计划/执行明细与计数
    /// * `Err(anyhow::Error)` - 读库失败等基础设施错误
    pub async fn restore(dto: DbzBackupRestoreDto) -> Result<Ro<DbzBackupRestoreVo>, anyhow::Error> {
        let strategy = restore_strategy(&dto.strategy);
        let (file, errors) = parse_backup(&dto.file);
        let Some(file) = file else {
            return Ok(Ro::success("备份文件校验未通过".to_string()).extra(Some(DbzBackupRestoreVo {
                valid: false,
                errors,
                strategy: strategy.to_string(),
                dry_run: dto.dry_run,
                ..Default::default()
            })));
        };

        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;
        let out = apply_restore(db.as_ref(), &file, strategy, dto.dry_run).await?;

        let action = if dto.dry_run { "预览" } else { "恢复" };
        let msg = format!(
            "{action}完成：新增 {}，覆盖 {}，跳过 {}，失败 {}",
            out.created, out.overwritten, out.skipped, out.failed
        );
        if !dto.dry_run {
            info!("{msg}（集群「{}」）", file.broker.name);
        }
        Ok(Ro::success(msg).extra(Some(out)))
    }
}

/// 恢复策略：只认 `overwrite`，其余一律按 `skip`（仅补齐缺失）
fn restore_strategy(raw: &str) -> &'static str {
    if raw.trim().eq_ignore_ascii_case("overwrite") {
        "overwrite"
    } else {
        "skip"
    }
}

/// 导出顺序：上游在前、下游在后，与 Connect 导入的处理顺序一致
fn connector_order(direction: &str) -> u8 {
    if direction == "source" {
        0
    } else {
        1
    }
}

/// 整理一条导出连接器
fn exported_connector(
    direction: &str,
    name: &str,
    connector_name: &str,
    connector_config: &str,
    remark: Option<&str>,
    enabled: bool,
    upstream: Option<DbzBackupUpstreamVo>,
) -> DbzBackupConnectorVo {
    DbzBackupConnectorVo {
        direction: direction.to_string(),
        name: name.to_string(),
        connector_name: connector_name.to_string(),
        connector_config: connector_config.to_string(),
        remark: remark.map(str::to_string),
        enabled,
        upstream,
        sensitive_config_keys: sensitive_keys(connector_config),
    }
}

/// 列出配置里的凭据字段：键名不区分大小写，`sasl.jaas.config` 整段算敏感
fn sensitive_keys(config: &str) -> Vec<String> {
    let Ok(Value::Object(map)) = serde_json::from_str::<Value>(config) else {
        return Vec::new();
    };
    map.keys()
        .filter(|key| is_sensitive_key(key))
        .cloned()
        .collect()
}

fn is_sensitive_key(key: &str) -> bool {
    let normalized = key.trim().to_ascii_lowercase();
    normalized == "sasl.jaas.config"
        || SENSITIVE_KEY_PATTERN
            .iter()
            .any(|pattern| normalized.contains(pattern))
}

/// 配置是否是合法的 JSON 对象
fn is_config_object(raw: &str) -> bool {
    matches!(serde_json::from_str::<Value>(raw), Ok(Value::Object(_)))
}

/// 取字符串字段并去掉首尾空白
fn text_of(map: &Map<String, Value>, key: &str) -> String {
    map.get(key)
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string()
}

/// 取可选字符串：空串按「没有」处理
fn optional_text(map: &Map<String, Value>, key: &str) -> Option<String> {
    let value = text_of(map, key);
    if value.is_empty() { None } else { Some(value) }
}

/// 取布尔字段
fn bool_of(map: &Map<String, Value>, key: &str, default: bool) -> bool {
    map.get(key).and_then(Value::as_bool).unwrap_or(default)
}

/// 取小整数字段（字典判别式）
fn i16_of(map: &Map<String, Value>, key: &str) -> i16 {
    map.get(key).and_then(Value::as_i64).unwrap_or(0) as i16
}

/// # 校验并解析备份文件
///
/// 返回 `(文件, 错误列表)`：有任何错误时文件为 `None`，错误列表逐条给出原因，
/// 与 Connect 导入的文件校验口径保持一致。
fn parse_backup(value: &Value) -> (Option<DbzBackupFileVo>, Vec<String>) {
    let mut errors: Vec<String> = Vec::new();
    let Some(root) = value.as_object() else {
        return (None, vec!["文件内容不是一个 JSON 对象".to_string()]);
    };

    if text_of(root, "format") != BACKUP_FORMAT {
        errors.push(format!(
            "不是 dbz 集群备份文件（format 应为 {BACKUP_FORMAT}）"
        ));
    }
    if root.get("version").and_then(Value::as_u64) != Some(BACKUP_VERSION as u64) {
        errors.push(format!(
            "不支持的导出版本 {}，当前只支持 {BACKUP_VERSION}",
            root.get("version").map(|item| item.to_string()).unwrap_or_else(|| "缺失".to_string())
        ));
    }

    let broker = root
        .get("broker")
        .and_then(Value::as_object)
        .and_then(|value| parse_broker(value, &mut errors));
    let connects = parse_connects(root, &mut errors);

    if errors.is_empty() {
        if let (Some(broker), Some(connects)) = (broker, connects) {
            return (
                Some(DbzBackupFileVo {
                    format: BACKUP_FORMAT.to_string(),
                    version: BACKUP_VERSION,
                    exported_at: text_of(root, "exportedAt"),
                    credential_mode: "included".to_string(),
                    broker,
                    connects,
                }),
                Vec::new(),
            );
        }
    }
    (None, errors)
}

/// 解析集群条目
fn parse_broker(map: &Map<String, Value>, errors: &mut Vec<String>) -> Option<DbzBackupBrokerVo> {
    let name = text_of(map, "name");
    let bootstrap_servers = text_of(map, "bootstrapServers");
    if name.is_empty() {
        errors.push("Kafka 集群缺少名称 name".to_string());
    }
    if bootstrap_servers.is_empty() {
        errors.push("Kafka 集群缺少地址 bootstrapServers".to_string());
    }
    if name.is_empty() || bootstrap_servers.is_empty() {
        return None;
    }
    Some(DbzBackupBrokerVo {
        name,
        bootstrap_servers,
        security_protocol: i16_of(map, "securityProtocol"),
        sasl_mechanism: i16_of(map, "saslMechanism"),
        sasl_username: optional_text(map, "saslUsername"),
        sasl_password: optional_text(map, "saslPassword"),
        remark: optional_text(map, "remark"),
        enabled: bool_of(map, "enabled", true),
    })
}

/// 解析全部 Connect；文件内 Connect 地址/名称与连接器名称都要求唯一
fn parse_connects(
    root: &Map<String, Value>,
    errors: &mut Vec<String>,
) -> Option<Vec<DbzBackupConnectVo>> {
    let Some(raw_connects) = root.get("connects").and_then(Value::as_array) else {
        errors.push("文件缺少 connects 数组".to_string());
        return None;
    };

    let mut connects: Vec<DbzBackupConnectVo> = Vec::new();
    let mut connect_urls = std::collections::HashSet::new();
    let mut connect_names = std::collections::HashSet::new();
    let mut connector_names = std::collections::HashSet::new();

    for (index, raw) in raw_connects.iter().enumerate() {
        let label = format!("第 {} 台 Connect", index + 1);
        let Some(map) = raw.as_object() else {
            errors.push(format!("{label}不是对象"));
            continue;
        };
        let name = text_of(map, "name");
        let url = text_of(map, "url");
        if name.is_empty() {
            errors.push(format!("{label}缺少名称 name"));
        }
        if url.is_empty() {
            errors.push(format!("{label}缺少地址 url"));
        }
        if name.is_empty() || url.is_empty() {
            continue;
        }
        if !connect_names.insert(name.clone()) {
            errors.push(format!("文件内 Connect 名称「{name}」重复"));
        }
        if !connect_urls.insert(normalize_url(&url)) {
            errors.push(format!("文件内 Connect 地址「{url}」重复"));
        }

        let Some(raw_connectors) = map.get("connectors").and_then(Value::as_array) else {
            errors.push(format!("{label}缺少 connectors 数组"));
            continue;
        };
        let mut connectors: Vec<DbzBackupConnectorVo> = Vec::new();
        for (connector_index, raw_connector) in raw_connectors.iter().enumerate() {
            let Some(connector) =
                parse_connector(raw_connector, connector_index, errors)
            else {
                continue;
            };
            if !connector_names.insert(connector.connector_name.clone()) {
                errors.push(format!(
                    "文件内 connectorName「{}」重复，无法确定导入顺序",
                    connector.connector_name
                ));
                continue;
            }
            connectors.push(connector);
        }

        connects.push(DbzBackupConnectVo {
            name,
            url,
            remark: optional_text(map, "remark"),
            enabled: bool_of(map, "enabled", true),
            connectors,
        });
    }

    Some(connects)
}

/// 解析一条连接器
fn parse_connector(
    value: &Value,
    index: usize,
    errors: &mut Vec<String>,
) -> Option<DbzBackupConnectorVo> {
    let label = format!("第 {} 条连接器", index + 1);
    let Some(map) = value.as_object() else {
        errors.push(format!("{label}不是对象"));
        return None;
    };
    let direction = text_of(map, "direction");
    if direction != "source" && direction != "sink" {
        errors.push(format!("{label}的方向必须是 source 或 sink"));
        return None;
    }
    let name = text_of(map, "name");
    let connector_name = text_of(map, "connectorName");
    if name.is_empty() {
        errors.push(format!("{label}缺少展示名称 name"));
    }
    if connector_name.is_empty() {
        errors.push(format!("{label}缺少 connectorName"));
    }

    let connector_config = match map.get("connectorConfig") {
        Some(Value::String(raw)) => raw.clone(),
        Some(value @ Value::Object(_)) => value.to_string(),
        Some(_) => {
            errors.push(format!("{label}的 connectorConfig 必须是 JSON 字符串或对象"));
            String::new()
        }
        None => {
            errors.push(format!("{label}缺少 connectorConfig"));
            String::new()
        }
    };
    if !connector_config.is_empty() && !is_config_object(&connector_config) {
        errors.push(format!(
            "{label}「{}」的 connectorConfig 不是合法的 JSON 对象",
            if name.is_empty() { &connector_name } else { &name }
        ));
    }

    let upstream = if direction == "sink" {
        let raw_upstream = map.get("upstream").and_then(Value::as_object);
        let upstream_name = raw_upstream
            .map(|item| text_of(item, "connectorName"))
            .unwrap_or_default();
        if upstream_name.is_empty() {
            errors.push(format!(
                "{label}「{}」是下游连接器，但缺少上游信息（upstream.connectorName）",
                if name.is_empty() { &connector_name } else { &name }
            ));
            None
        } else {
            Some(DbzBackupUpstreamVo {
                id: raw_upstream.and_then(|item| optional_text(item, "id")),
                name: raw_upstream
                    .map(|item| text_of(item, "name"))
                    .unwrap_or_default(),
                connector_name: upstream_name,
            })
        }
    } else {
        None
    };

    if name.is_empty() || connector_name.is_empty() || connector_config.is_empty() {
        return None;
    }

    let mut sensitive: Vec<String> = map
        .get("sensitiveConfigKeys")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    if sensitive.is_empty() {
        sensitive = sensitive_keys(&connector_config);
    }

    Some(DbzBackupConnectorVo {
        direction,
        name,
        connector_name,
        connector_config,
        remark: optional_text(map, "remark"),
        enabled: bool_of(map, "enabled", true),
        upstream,
        sensitive_config_keys: sensitive,
    })
}

/// # 执行（或预览）恢复
async fn apply_restore(
    db: &DatabaseConnection,
    file: &DbzBackupFileVo,
    strategy: &str,
    dry_run: bool,
) -> Result<DbzBackupRestoreVo, anyhow::Error> {
    let mut brokers = DbzKafkaBrokerDao::list_by_condition::<_, BrokerModel>(
        Condition::all(),
        &None,
        db,
    )
    .await
    .map_err(|e| anyhow!("查询 Kafka 集群列表失败: {e}"))?;
    let mut connects = DbzKafkaConnectDao::list_by_condition::<_, ConnectModel>(
        Condition::all(),
        &None,
        db,
    )
    .await
    .map_err(|e| anyhow!("查询 Kafka Connect 列表失败: {e}"))?;
    let mut sources = DbzSourceConnectorDao::list_by_condition::<_, SourceModel>(
        Condition::all(),
        &None,
        db,
    )
    .await
    .map_err(|e| anyhow!("查询上游连接器列表失败: {e}"))?;
    let mut sinks = DbzSinkConnectorDao::list_by_condition::<_, SinkModel>(
        Condition::all(),
        &None,
        db,
    )
    .await
    .map_err(|e| anyhow!("查询下游连接器列表失败: {e}"))?;

    let mut out = DbzBackupRestoreVo {
        valid: true,
        strategy: strategy.to_string(),
        dry_run,
        ..Default::default()
    };

    let broker_id = restore_broker(
        db, &mut brokers, &file.broker, strategy, dry_run, &mut out,
    )
    .await?;

    let Some(broker_id) = broker_id else {
        for connect in &file.connects {
            push_item(
                &mut out,
                "connect",
                Some(file.broker.name.clone()),
                &connect.name,
                None,
                "failed",
                Some("所属 Kafka 集群未能恢复".to_string()),
            );
            for connector in &connect.connectors {
                push_item(
                    &mut out,
                    &connector.direction,
                    Some(connect.name.clone()),
                    &connector.name,
                    Some(&connector.connector_name),
                    "failed",
                    Some("所属 Kafka 集群未能恢复".to_string()),
                );
            }
        }
        return Ok(out);
    };

    // Connect 先全部落位：连接器需要挂到已经恢复（或已存在）的 Connect 上
    let mut connect_ids: Vec<(String, Option<i64>)> = Vec::new();
    for connect in &file.connects {
        let id = restore_connect(
            db,
            &mut connects,
            broker_id,
            connect,
            strategy,
            dry_run,
            &mut out,
        )
        .await?;
        connect_ids.push((connect.name.clone(), id));
    }

    // 上游 → 下游的固定顺序，下游按 upstream.connectorName 重新挂载
    let mut source_ids: HashMap<String, i64> = sources
        .iter()
        .map(|item| (item.connector_name.clone(), item.id))
        .collect();

    for (connect_index, connect) in file.connects.iter().enumerate() {
        let connect_id = connect_ids[connect_index].1;
        for connector in connect
            .connectors
            .iter()
            .filter(|item| item.direction == "source")
        {
            let Some(id) = restore_source(
                db,
                &mut sources,
                &sinks,
                connect_id,
                &connect.name,
                connector,
                strategy,
                dry_run,
                &mut out,
            )
            .await?
            else {
                continue;
            };
            source_ids.insert(connector.connector_name.clone(), id);
        }
    }

    for (connect_index, connect) in file.connects.iter().enumerate() {
        let connect_id = connect_ids[connect_index].1;
        for connector in connect
            .connectors
            .iter()
            .filter(|item| item.direction == "sink")
        {
            restore_sink(
                db,
                &mut sinks,
                &sources,
                &source_ids,
                connect_id,
                &connect.name,
                connector,
                strategy,
                dry_run,
                &mut out,
            )
            .await?;
        }
    }

    Ok(out)
}

/// 记录一条明细并累加计数
fn push_item(
    out: &mut DbzBackupRestoreVo,
    level: &str,
    parent: Option<String>,
    name: &str,
    connector_name: Option<&str>,
    action: &str,
    message: Option<String>,
) {
    match action {
        "create" => out.created += 1,
        "overwrite" => out.overwritten += 1,
        "skip" => out.skipped += 1,
        _ => out.failed += 1,
    }
    out.items.push(DbzBackupRestoreItemVo {
        level: level.to_string(),
        parent,
        name: name.to_string(),
        connector_name: connector_name.map(str::to_string),
        action: action.to_string(),
        message,
    });
}

/// 恢复 Kafka 集群：按地址优先、名称兜底匹配；返回后续记录要挂的集群 ID
#[allow(clippy::too_many_arguments)]
async fn restore_broker(
    db: &DatabaseConnection,
    brokers: &mut Vec<BrokerModel>,
    file: &DbzBackupBrokerVo,
    strategy: &str,
    dry_run: bool,
    out: &mut DbzBackupRestoreVo,
) -> Result<Option<i64>, anyhow::Error> {
    let address = file.bootstrap_servers.trim();
    let existing = brokers
        .iter()
        .position(|item| item.bootstrap_servers.trim() == address)
        .or_else(|| brokers.iter().position(|item| item.name == file.name));

    if let Some(index) = existing {
        let id = brokers[index].id;
        if strategy == "skip" {
            push_item(out, "broker", None, &file.name, None, "skip", None);
            return Ok(Some(id));
        }
        if brokers.iter().any(|item| {
            item.id != id && (item.name == file.name || item.bootstrap_servers.trim() == address)
        }) {
            push_item(
                out,
                "broker",
                None,
                &file.name,
                None,
                "failed",
                Some("名称或地址已被另一条集群记录占用".to_string()),
            );
            return Ok(Some(id));
        }
        if !dry_run {
            let mut active = BrokerActiveModel {
                id: Set(id),
                ..Default::default()
            };
            active.name = Set(file.name.clone());
            active.bootstrap_servers = Set(file.bootstrap_servers.clone());
            active.security_protocol = Set(file.security_protocol);
            active.sasl_mechanism = Set(file.sasl_mechanism);
            active.sasl_username = Set(file.sasl_username.clone());
            active.sasl_password = Set(file.sasl_password.clone());
            active.remark = Set(file.remark.clone());
            active.enabled = Set(file.enabled);
            active.run_status = Set(KafkaBrokerStatus::None.value());
            active.run_msg = Set(None);
            active.updater_id = Set(0);
            match DbzKafkaBrokerDao::update(active, db).await {
                Ok(model) => brokers[index] = model,
                Err(e) => {
                    push_item(
                        out,
                        "broker",
                        None,
                        &file.name,
                        None,
                        "failed",
                        Some(format!("覆盖失败：{e}")),
                    );
                    return Ok(Some(id));
                }
            }
        }
        push_item(out, "broker", None, &file.name, None, "overwrite", None);
        return Ok(Some(id));
    }

    // 新增：名称、地址都要求唯一
    if brokers
        .iter()
        .any(|item| item.name == file.name || item.bootstrap_servers.trim() == address)
    {
        push_item(
            out,
            "broker",
            None,
            &file.name,
            None,
            "failed",
            Some("名称或地址已被另一条集群记录占用".to_string()),
        );
        return Ok(None);
    }
    if dry_run {
        push_item(out, "broker", None, &file.name, None, "create", None);
        return Ok(Some(-1));
    }

    let active = BrokerActiveModel {
        name: Set(file.name.clone()),
        bootstrap_servers: Set(file.bootstrap_servers.clone()),
        security_protocol: Set(file.security_protocol),
        sasl_mechanism: Set(file.sasl_mechanism),
        sasl_username: Set(file.sasl_username.clone()),
        sasl_password: Set(file.sasl_password.clone()),
        run_status: Set(KafkaBrokerStatus::None.value()),
        run_msg: Set(None),
        remark: Set(file.remark.clone()),
        enabled: Set(file.enabled),
        creater_id: Set(0),
        ..Default::default()
    };
    match DbzKafkaBrokerDao::insert(active, db).await {
        Ok(model) => {
            let id = model.id;
            brokers.push(model);
            push_item(out, "broker", None, &file.name, None, "create", None);
            Ok(Some(id))
        }
        Err(e) => {
            push_item(
                out,
                "broker",
                None,
                &file.name,
                None,
                "failed",
                Some(format!("新增失败：{e}")),
            );
            Ok(None)
        }
    }
}

/// 恢复一台 Kafka Connect：按地址优先、名称兜底匹配
#[allow(clippy::too_many_arguments)]
async fn restore_connect(
    db: &DatabaseConnection,
    connects: &mut Vec<ConnectModel>,
    broker_id: i64,
    file: &DbzBackupConnectVo,
    strategy: &str,
    dry_run: bool,
    out: &mut DbzBackupRestoreVo,
) -> Result<Option<i64>, anyhow::Error> {
    let url = normalize_url(&file.url);
    let existing = connects
        .iter()
        .position(|item| normalize_url(&item.url) == url)
        .or_else(|| connects.iter().position(|item| item.name == file.name));

    if let Some(index) = existing {
        let id = connects[index].id;
        if strategy == "skip" {
            push_item(out, "connect", None, &file.name, None, "skip", None);
            return Ok(Some(id));
        }
        if connects
            .iter()
            .any(|item| item.id != id && (item.name == file.name || normalize_url(&item.url) == url))
        {
            push_item(
                out,
                "connect",
                None,
                &file.name,
                None,
                "failed",
                Some("名称或地址已被另一台 Connect 记录占用".to_string()),
            );
            return Ok(Some(id));
        }
        if !dry_run {
            let mut active = ConnectActiveModel {
                id: Set(id),
                ..Default::default()
            };
            active.kafka_broker_id = Set(broker_id);
            active.name = Set(file.name.clone());
            active.url = Set(file.url.clone());
            active.remark = Set(file.remark.clone());
            active.enabled = Set(file.enabled);
            active.run_status = Set(KafkaConnectStatus::None.value());
            active.run_msg = Set(None);
            active.updater_id = Set(0);
            match DbzKafkaConnectDao::update(active, db).await {
                Ok(model) => connects[index] = model,
                Err(e) => {
                    push_item(
                        out,
                        "connect",
                        None,
                        &file.name,
                        None,
                        "failed",
                        Some(format!("覆盖失败：{e}")),
                    );
                    return Ok(Some(id));
                }
            }
        }
        push_item(out, "connect", None, &file.name, None, "overwrite", None);
        return Ok(Some(id));
    }

    if connects
        .iter()
        .any(|item| item.name == file.name || normalize_url(&item.url) == url)
    {
        push_item(
            out,
            "connect",
            None,
            &file.name,
            None,
            "failed",
            Some("名称或地址已被另一台 Connect 记录占用".to_string()),
        );
        return Ok(None);
    }
    if dry_run {
        push_item(out, "connect", None, &file.name, None, "create", None);
        return Ok(Some(-1));
    }

    let active = ConnectActiveModel {
        kafka_broker_id: Set(broker_id),
        name: Set(file.name.clone()),
        url: Set(file.url.clone()),
        run_status: Set(KafkaConnectStatus::None.value()),
        run_msg: Set(None),
        remark: Set(file.remark.clone()),
        enabled: Set(file.enabled),
        creater_id: Set(0),
        ..Default::default()
    };
    match DbzKafkaConnectDao::insert(active, db).await {
        Ok(model) => {
            let id = model.id;
            connects.push(model);
            push_item(out, "connect", None, &file.name, None, "create", None);
            Ok(Some(id))
        }
        Err(e) => {
            push_item(
                out,
                "connect",
                None,
                &file.name,
                None,
                "failed",
                Some(format!("新增失败：{e}")),
            );
            Ok(None)
        }
    }
}

/// 恢复一条上游连接器；返回可被下游挂载的记录 ID
#[allow(clippy::too_many_arguments)]
async fn restore_source(
    db: &DatabaseConnection,
    sources: &mut Vec<SourceModel>,
    sinks: &[SinkModel],
    connect_id: Option<i64>,
    connect_name: &str,
    file: &DbzBackupConnectorVo,
    strategy: &str,
    dry_run: bool,
    out: &mut DbzBackupRestoreVo,
) -> Result<Option<i64>, anyhow::Error> {
    let parent = Some(connect_name.to_string());
    let Some(connect_id) = connect_id else {
        push_item(
            out,
            "source",
            parent,
            &file.name,
            Some(&file.connector_name),
            "failed",
            Some("所属 Kafka Connect 未能恢复".to_string()),
        );
        return Ok(None);
    };

    if let Some(index) = sources
        .iter()
        .position(|item| item.connector_name == file.connector_name)
    {
        let id = sources[index].id;
        if strategy == "skip" {
            push_item(
                out,
                "source",
                parent,
                &file.name,
                Some(&file.connector_name),
                "skip",
                None,
            );
            return Ok(Some(id));
        }
        if sources
            .iter()
            .any(|item| item.id != id && item.name == file.name)
        {
            push_item(
                out,
                "source",
                parent,
                &file.name,
                Some(&file.connector_name),
                "failed",
                Some("名称已被另一条上游连接器记录占用".to_string()),
            );
            return Ok(Some(id));
        }
        if !dry_run {
            let mut active = SourceActiveModel {
                id: Set(id),
                ..Default::default()
            };
            active.kafka_connect_id = Set(connect_id);
            active.name = Set(file.name.clone());
            active.connector_config = Set(file.connector_config.clone());
            active.remark = Set(file.remark.clone());
            active.enabled = Set(file.enabled);
            active.run_status = Set(ConnectorRunStatus::Unknown.value());
            active.run_msg = Set(None);
            active.updater_id = Set(0);
            match DbzSourceConnectorDao::update(active, db).await {
                Ok(model) => sources[index] = model,
                Err(e) => {
                    push_item(
                        out,
                        "source",
                        parent,
                        &file.name,
                        Some(&file.connector_name),
                        "failed",
                        Some(format!("覆盖失败：{e}")),
                    );
                    return Ok(Some(id));
                }
            }
        }
        push_item(
            out,
            "source",
            parent,
            &file.name,
            Some(&file.connector_name),
            "overwrite",
            None,
        );
        return Ok(Some(id));
    }

    if sources.iter().any(|item| item.name == file.name)
        || sinks
            .iter()
            .any(|item| item.connector_name == file.connector_name)
    {
        push_item(
            out,
            "source",
            parent,
            &file.name,
            Some(&file.connector_name),
            "failed",
            Some("名称或连接器名称已被另一条记录占用".to_string()),
        );
        return Ok(None);
    }
    if dry_run {
        push_item(
            out,
            "source",
            parent,
            &file.name,
            Some(&file.connector_name),
            "create",
            None,
        );
        return Ok(Some(-1));
    }

    let active = SourceActiveModel {
        kafka_connect_id: Set(connect_id),
        name: Set(file.name.clone()),
        connector_name: Set(file.connector_name.clone()),
        connector_config: Set(file.connector_config.clone()),
        run_status: Set(ConnectorRunStatus::Unknown.value()),
        run_msg: Set(None),
        remark: Set(file.remark.clone()),
        enabled: Set(file.enabled),
        creater_id: Set(0),
        ..Default::default()
    };
    match DbzSourceConnectorDao::insert(active, db).await {
        Ok(model) => {
            let id = model.id;
            sources.push(model);
            push_item(
                out,
                "source",
                parent,
                &file.name,
                Some(&file.connector_name),
                "create",
                None,
            );
            Ok(Some(id))
        }
        Err(e) => {
            push_item(
                out,
                "source",
                parent,
                &file.name,
                Some(&file.connector_name),
                "failed",
                Some(format!("新增失败：{e}")),
            );
            Ok(None)
        }
    }
}

/// 恢复一条下游连接器：上游按文件里的 `connectorName` 重新挂载
#[allow(clippy::too_many_arguments)]
async fn restore_sink(
    db: &DatabaseConnection,
    sinks: &mut Vec<SinkModel>,
    sources: &[SourceModel],
    source_ids: &HashMap<String, i64>,
    connect_id: Option<i64>,
    connect_name: &str,
    file: &DbzBackupConnectorVo,
    strategy: &str,
    dry_run: bool,
    out: &mut DbzBackupRestoreVo,
) -> Result<(), anyhow::Error> {
    let parent = Some(connect_name.to_string());
    let Some(connect_id) = connect_id else {
        push_item(
            out,
            "sink",
            parent,
            &file.name,
            Some(&file.connector_name),
            "failed",
            Some("所属 Kafka Connect 未能恢复".to_string()),
        );
        return Ok(());
    };
    let upstream_name = file
        .upstream
        .as_ref()
        .map(|item| item.connector_name.as_str())
        .unwrap_or("");
    let Some(upstream_id) = source_ids.get(upstream_name).copied() else {
        push_item(
            out,
            "sink",
            parent,
            &file.name,
            Some(&file.connector_name),
            "failed",
            Some(format!(
                "缺少上游连接器「{}」（connectorName={upstream_name}），库内没有对应的纳管记录",
                file.upstream
                    .as_ref()
                    .map(|item| item.name.as_str())
                    .unwrap_or(upstream_name)
            )),
        );
        return Ok(());
    };

    if let Some(index) = sinks
        .iter()
        .position(|item| item.connector_name == file.connector_name)
    {
        let id = sinks[index].id;
        if strategy == "skip" {
            push_item(
                out,
                "sink",
                parent,
                &file.name,
                Some(&file.connector_name),
                "skip",
                None,
            );
            return Ok(());
        }
        if sinks
            .iter()
            .any(|item| item.id != id && item.name == file.name)
        {
            push_item(
                out,
                "sink",
                parent,
                &file.name,
                Some(&file.connector_name),
                "failed",
                Some("名称已被另一条下游连接器记录占用".to_string()),
            );
            return Ok(());
        }
        if !dry_run {
            let mut active = SinkActiveModel {
                id: Set(id),
                ..Default::default()
            };
            active.kafka_connect_id = Set(connect_id);
            active.source_connector_id = Set(upstream_id);
            active.name = Set(file.name.clone());
            active.connector_config = Set(file.connector_config.clone());
            active.remark = Set(file.remark.clone());
            active.enabled = Set(file.enabled);
            active.run_status = Set(ConnectorRunStatus::Unknown.value());
            active.run_msg = Set(None);
            active.updater_id = Set(0);
            match DbzSinkConnectorDao::update(active, db).await {
                Ok(model) => sinks[index] = model,
                Err(e) => {
                    push_item(
                        out,
                        "sink",
                        parent,
                        &file.name,
                        Some(&file.connector_name),
                        "failed",
                        Some(format!("覆盖失败：{e}")),
                    );
                    return Ok(());
                }
            }
        }
        push_item(
            out,
            "sink",
            parent,
            &file.name,
            Some(&file.connector_name),
            "overwrite",
            None,
        );
        return Ok(());
    }

    if sinks.iter().any(|item| item.name == file.name)
        || sources
            .iter()
            .any(|item| item.connector_name == file.connector_name)
    {
        push_item(
            out,
            "sink",
            parent,
            &file.name,
            Some(&file.connector_name),
            "failed",
            Some("名称或连接器名称已被另一条记录占用".to_string()),
        );
        return Ok(());
    }
    if dry_run {
        push_item(
            out,
            "sink",
            parent,
            &file.name,
            Some(&file.connector_name),
            "create",
            None,
        );
        return Ok(());
    }

    let active = SinkActiveModel {
        kafka_connect_id: Set(connect_id),
        source_connector_id: Set(upstream_id),
        name: Set(file.name.clone()),
        connector_name: Set(file.connector_name.clone()),
        connector_config: Set(file.connector_config.clone()),
        run_status: Set(ConnectorRunStatus::Unknown.value()),
        run_msg: Set(None),
        remark: Set(file.remark.clone()),
        enabled: Set(file.enabled),
        creater_id: Set(0),
        ..Default::default()
    };
    match DbzSinkConnectorDao::insert(active, db).await {
        Ok(model) => {
            sinks.push(model);
            push_item(
                out,
                "sink",
                parent,
                &file.name,
                Some(&file.connector_name),
                "create",
                None,
            );
        }
        Err(e) => {
            push_item(
                out,
                "sink",
                parent,
                &file.name,
                Some(&file.connector_name),
                "failed",
                Some(format!("新增失败：{e}")),
            );
        }
    }
    Ok(())
}
