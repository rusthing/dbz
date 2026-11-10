//! # 死信服务（Kafka Connect DLQ 镜像 + 人工处置）
//!
//! ## 职责边界
//!
//! 坏记录的**产生、重试、保全**全部由 Kafka Connect 负责：部署下游连接器时，
//! [`crate::utils::ensure_sink_dlq_config`] 会把 `errors.tolerance=all`、
//! `errors.deadletterqueue.*`、`errors.retry.*` 写进 `connector_config`，Connect 于是把
//! 处理不了的记录连同 `__connect.errors.*` 上下文头写进 DLQ topic，并正常提交偏移。
//!
//! 本服务只做三件事：
//!
//! 1. **同步**：从 DLQ topic 增量拉取记录，按 `(connector_id, 源 topic, 分区, offset)`
//!    幂等落到 `dbz_dead_letter`，作为可查询、可筛选的页面索引。DLQ topic 本身才是
//!    事实来源，表里删了记录也不会丢 Kafka 里的原始报文。
//! 2. **处置**：待处理 / 已重投 / 已废弃 / 已人工处理，以及备注与处理人，这些是
//!    Connect 没有的「人的结论」，只存在 dbz。
//! 3. **重投**：把报文重新生产回**源 topic**，让 Connect 重新消费一次，而不是绕过
//!    Connect 直接 POST 下游 HTTP 地址。重试策略、转换器、SMT 与 offset 语义因而
//!    和原始链路完全一致。
//!
//! ## 不再做的事
//!
//! - 不再有「业务脚本上报失败」：`/report` 已删除。旧实现里脚本用 HTTP 200 +
//!   `{"result":-2}` 表达业务失败，Connect 认为成功并提交偏移，于是要求脚本主动调
//!   dbz 写死信。脚本漏报、崩溃、网络断都会静默丢数据，而且 Kafka 里没有原始报文。
//!   **新机制的前提是下游把业务失败表达成非 2xx**，否则 Connect 层看不到失败。
//! - 不再有自建重试队列与退避 worker：Connect 的 `errors.retry.timeout`、
//!   `errors.retry.delay.max.ms` 与 HTTP Sink 的 `max.retries` / `retry.backoff.ms`
//!   已经覆盖，dbz 再投一遍只会绕过框架、且无法保证顺序与幂等。
//! - 不再按 `(connector_type, connector_id, topic, recordKey)` 合并失败：DLQ 里
//!   每条 Kafka 记录都是独立事件，唯一键改成
//!   `(connector_id, topic, partition, offset)`，重复同步不会覆盖人工结论。带上
//!   `connector_id` 是因为同一源 topic 可能被多条 sink 连接器消费，同一条源记录在
//!   每条连接器上失败都是独立的一条死信，只按源坐标查重会互相吞掉。
//!
//! ## 适用范围
//!
//! Connect 的 DLQ 只对 **sink** 连接器有效（source 侧没有「把外部记录转成 Kafka
//! 记录」这一步），所以 `connector_type` 恒为 `Sink`；上游连接器不产生死信。

use crate::config::{DeadLetterConfig, get_dbz_config};
use crate::dao::DbzDeadLetterDao;
use crate::dic::{
    DeadLetterConnectorType, DeadLetterFailureCategory, DeadLetterFailureSubtype, DeadLetterStatus,
};
use crate::dto::{
    DbzDeadLetterCountDto, DbzDeadLetterDiscardDto, DbzDeadLetterEditPayloadDto,
    DbzDeadLetterMarkHandledDto, DbzDeadLetterQueryDto, DbzDeadLetterRetryDto,
    DbzDeadLetterUnmarkHandledDto,
};
use crate::mo::dbz_dead_letter::{
    ActiveModel as DeadLetterActiveModel, Column as DeadLetterColumn, Entity as DeadLetterEntity,
    Model as DeadLetterModel,
};
use crate::mo::dbz_kafka_broker::{
    Column as BrokerColumn, Entity as BrokerEntity, Model as BrokerModel,
};
use crate::mo::dbz_kafka_connect::Entity as ConnectEntity;
use crate::mo::dbz_sink_connector::{Column as SinkColumn, Entity as SinkEntity};
use crate::utils::{
    BrokerConn, HDR_CONNECTOR_NAME, HDR_EXCEPTION_CLASS, HDR_EXCEPTION_MESSAGE,
    HDR_EXCEPTION_STACKTRACE, HDR_OFFSET, HDR_PARTITION, HDR_TOPIC, KafkaBrokerUtils,
    classify_error, dlq_topic, header_text, payload_bytes, payload_value,
};
use crate::vo::{DbzDeadLetterCountVo, DbzDeadLetterRetryResultVo, DbzDeadLetterVo};
use anyhow::anyhow;
use robotech::api::{PageRx, Ro, U64};
use robotech::db::get_db_conn;
use robotech::svc::SvcError;
use rskafka::client::partition::{OffsetAt, UnknownTopicHandling};
use rskafka::record::Record;
use sea_orm::sea_query::Expr;
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseConnection, EntityTrait, QueryFilter,
    QuerySelect, Set,
};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::LazyLock;
use tokio::sync::Mutex;
use tracing::{info, warn};
use wheel_rs::time_utils::now_ms;

/// DLQ 各分区的同步进度：`{broker}|{topic}|{partition}` → 下一条要读的 offset
///
/// 只放在进程内，重启后按 `Earliest` 重新扫一遍：DLQ topic 有 retention，且落库按
/// 四列坐标幂等、遇到已存在的行直接跳过，重扫不会重复，也不会覆盖人工处置结论。
/// 进程内这把锁只用来串行化本进程的分区拉取；**跨副本**并发靠唯一索引 +
/// `upsert_from_dlq` 撞唯一键后的复查兜底，不需要分布式锁。
static DLQ_CURSORS: LazyLock<Mutex<HashMap<String, i64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// 保留清理周期：10 分钟一次
const CLEANUP_INTERVAL_MS: i64 = 10 * 60 * 1_000;

/// 单次 fetch 的最大字节范围
const FETCH_BYTES: std::ops::Range<i32> = 1..1_048_576;

/// # 死信服务
///
/// 没有对应的 CRUD 路由，只有同步、查询、重投、废弃、计数，因此不使用 `#[svc]` 宏。
pub struct DbzDeadLetterSvc;

// SvcError 由框架定义且体积较大，钩子/服务方法的返回类型必须与宏约定一致，
// 无法改成 Box 包裹，这里统一放行该 lint（与 dbz_sink_connector_svc 同一处理）
#[allow(clippy::result_large_err)]
impl DbzDeadLetterSvc {
    /// # 手动同步（页面「同步」按钮）
    ///
    /// jobx 周期任务走 [`Self::sync_from_dlq`]；这里包一层给 HTTP 用。
    pub async fn sync() -> Result<Ro<u64>, SvcError> {
        let count = Self::sync_from_dlq()
            .await
            .map_err(|e| SvcError::from(anyhow!("同步 Kafka Connect DLQ 失败: {e}")))?;
        Ok(Ro::success(format!("已同步 {count} 条死信")).extra(Some(count)))
    }

    /// # 从 Kafka Connect DLQ 增量同步死信
    ///
    /// 遍历 dbz 纳管的每个 Kafka 集群（按 broker 地址去重），拉取全局 DLQ topic 的
    /// 全部分区。同步失败不影响其它集群：单个集群连不上只记日志。
    ///
    /// 由 jobx 健康检查计划每轮顺带触发一次，页面「同步」按钮也能手动触发。
    ///
    /// ## 返回值
    /// * `Ok(u64)` - 本轮新落库的死信条数
    pub async fn sync_from_dlq() -> anyhow::Result<u64> {
        let cfg = current_config();
        if !cfg.enabled {
            return Ok(0);
        }
        let topic = dlq_topic(&cfg).to_string();
        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;
        let brokers = BrokerEntity::find()
            .filter(BrokerColumn::Enabled.eq(true))
            .all(db.as_ref())
            .await?;

        let mut seen = HashSet::new();
        let mut total = 0u64;
        for broker in brokers {
            if !seen.insert(broker.bootstrap_servers.clone()) {
                continue;
            }
            match Self::sync_one_cluster(db.as_ref(), &broker, &topic, &cfg).await {
                Ok(count) => total += count,
                Err(e) => warn!("从 Kafka 集群<{}>同步 DLQ<{}>失败: {e}", broker.name, topic),
            }
        }

        if let Err(e) = Self::maybe_cleanup(db.as_ref(), &cfg).await {
            warn!("死信保留清理失败: {e}");
        }
        Ok(total)
    }

    /// # 同步一个 Kafka 集群上的 DLQ topic
    async fn sync_one_cluster(
        db: &DatabaseConnection,
        broker: &BrokerModel,
        topic: &str,
        cfg: &DeadLetterConfig,
    ) -> anyhow::Result<u64> {
        let conn = BrokerConn::from_model(broker);
        let client = KafkaBrokerUtils::connect(&conn).await?;
        let topics = client.list_topics().await?;
        // DLQ topic 由 Connect 在第一次坏记录出现时自动创建，还没有死信时不存在是正常的
        let Some(metadata) = topics.iter().find(|one| one.name == topic) else {
            return Ok(0);
        };

        let mut total = 0u64;
        for partition in &metadata.partitions {
            let client = client
                .partition_client(topic, *partition, UnknownTopicHandling::Error)
                .await?;
            let earliest = client.get_offset(OffsetAt::Earliest).await?;
            let latest = client.get_offset(OffsetAt::Latest).await?;
            let cursor_key = format!("{}|{}|{}", broker.bootstrap_servers, topic, partition);
            let mut cursors = DLQ_CURSORS.lock().await;
            // DLQ topic 被删后重建 / retention 清到新起点时，进程内游标会跑到 latest 后面，
            // 这时必须从 earliest 重新扫，否则该分区永远同步不到新记录。
            let stored = cursors.get(&cursor_key).copied().unwrap_or(earliest);
            let start = if stored > latest {
                earliest
            } else {
                stored.max(earliest)
            };
            if start >= latest {
                continue;
            }
            let (records, _) = match client.fetch_records(start, FETCH_BYTES, 500).await {
                Ok(result) => result,
                Err(e) => {
                    drop(cursors);
                    return Err(anyhow!("拉取 DLQ<{topic}>分区<{partition}>失败: {e}"));
                }
            };
            let batch: Vec<_> = records
                .iter()
                .take(cfg.sync_batch_size.max(1) as usize)
                .collect();
            for record in &batch {
                // 只数真正新落库的条数：重扫命中的旧记录不该算进「已同步 N 条」
                if Self::upsert_from_dlq(db, topic, *partition, record).await? {
                    total += 1;
                }
            }
            if let Some(last) = batch.last() {
                cursors.insert(cursor_key, last.offset + 1);
            }
            drop(cursors);
        }
        Ok(total)
    }

    /// # 把一条 DLQ 记录落成页面索引
    ///
    /// 重复同步（多副本、进程重启后重扫）时按
    /// `(connector_id, topic, partition, offset)` 查到已存在就直接返回：DLQ 记录
    /// 是不可变的，重扫没有新信息，更不该把「已人工处理」重置回「待处理」。
    ///
    /// 返回是否**新增**了一行，供同步计数使用。
    async fn upsert_from_dlq(
        db: &DatabaseConnection,
        dlq_topic: &str,
        dlq_partition: i32,
        entry: &rskafka::record::RecordAndOffset,
    ) -> anyhow::Result<bool> {
        let headers = &entry.record.headers;
        // 没开 context headers 时拿不到源坐标，用 DLQ 自身坐标兜底，保证仍能入库
        let source_topic =
            header_text(headers, HDR_TOPIC).unwrap_or_else(|| format!("{dlq_topic}:no-context"));
        let source_partition = header_text(headers, HDR_PARTITION)
            .and_then(|text| text.parse::<i32>().ok())
            .unwrap_or(dlq_partition);
        let source_offset = header_text(headers, HDR_OFFSET)
            .and_then(|text| text.parse::<i64>().ok())
            .unwrap_or(entry.offset);
        let connector_name = header_text(headers, HDR_CONNECTOR_NAME).unwrap_or_default();
        let exception_class = header_text(headers, HDR_EXCEPTION_CLASS).unwrap_or_default();
        let exception_message = header_text(headers, HDR_EXCEPTION_MESSAGE).unwrap_or_default();
        let stacktrace = header_text(headers, HDR_EXCEPTION_STACKTRACE).unwrap_or_default();

        // 幂等键必须包含连接器：同一源 topic 常被多条 sink 连接器订阅，同一条源记录
        // 在每条连接器上失败都会各自写一条 DLQ 记录，只按源坐标查重会互相吞掉。
        // DLQ 头里的连接器名反查不到本地记录时记 0（未归属），此时源坐标仍可区分。
        let connector_id = Self::find_sink_id(db, &connector_name).await;
        if Self::dead_letter_exists(
            db,
            connector_id,
            &source_topic,
            source_partition,
            source_offset,
        )
        .await?
        {
            return Ok(false);
        }

        let (category, subtype) = classify_error(&exception_class, &exception_message);
        let category = i16::from(category);
        let subtype = i16::from(subtype);
        let detail = format!(
            "Kafka Connect DLQ<{dlq_topic}:{dlq_partition}:{}>\n异常类: {exception_class}\n异常消息: {exception_message}\n堆栈: {stacktrace}",
            entry.offset
        );
        let payload = payload_value(entry.record.value.as_deref().unwrap_or_default());
        let record_key = entry
            .record
            .key
            .as_deref()
            .map(|bytes| truncate(&String::from_utf8_lossy(bytes), 512))
            .unwrap_or_default();
        let occurred_ms = entry.record.timestamp.timestamp_millis();
        let now = now_ms() as i64;

        let active = DeadLetterActiveModel {
            connector_type: Set(i16::from(DeadLetterConnectorType::Sink)),
            connector_id: Set(connector_id),
            topic: Set(source_topic.clone()),
            record_key: Set(record_key),
            partition: Set(Some(source_partition)),
            offset: Set(Some(source_offset)),
            payload: Set(payload),
            failure_category: Set(category),
            failure_subtype: Set(subtype),
            reason: Set(Self::dead_letter_reason(category, subtype)),
            failure_detail: Set(detail),
            retry_count: Set(0),
            first_seen_ms: Set(occurred_ms),
            last_seen_ms: Set(occurred_ms),
            status: Set(i16::from(DeadLetterStatus::Pending)),
            handled_by: Set(None),
            handled_ms: Set(None),
            remark: Set(None),
            pending_payload: Set(None),
            payload_edited_by: Set(None),
            payload_edited_ms: Set(None),
            creater_id: Set(0),
            create_ms: Set(now),
            updater_id: Set(0),
            update_ms: Set(now),
            ..Default::default()
        };
        if let Err(e) = DbzDeadLetterDao::insert(active, db).await {
            // 多副本的 jobx 可能同时同步到同一条 DLQ 记录，落库会撞唯一索引。
            // 复查到行就当成本轮跳过；其它错误照常上抛，不吞真实故障。
            if Self::dead_letter_exists(
                db,
                connector_id,
                &source_topic,
                source_partition,
                source_offset,
            )
            .await?
            {
                return Ok(false);
            }
            return Err(e.into());
        }
        info!(
            "同步 Connect 死信: connector={connector_name}(id={connector_id}) dlq={dlq_topic}:{dlq_partition}:{} source={source_partition}/{source_offset}",
            entry.offset
        );
        Ok(true)
    }

    /// 按四列幂等键查重；查重与插入之间不持分布式锁，靠这里的复查兜并发
    async fn dead_letter_exists(
        db: &DatabaseConnection,
        connector_id: i64,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> anyhow::Result<bool> {
        Ok(DeadLetterEntity::find()
            .filter(
                Condition::all()
                    .add(DeadLetterColumn::ConnectorId.eq(connector_id))
                    .add(DeadLetterColumn::Topic.eq(topic))
                    .add(DeadLetterColumn::Partition.eq(partition))
                    .add(DeadLetterColumn::Offset.eq(offset)),
            )
            .one(db)
            .await?
            .is_some())
    }

    /// 按 Connect 头里的连接器名反查本地下游连接器 ID，用于页面角标与筛选
    async fn find_sink_id(db: &DatabaseConnection, connector_name: &str) -> i64 {
        if connector_name.is_empty() {
            return 0;
        }
        SinkEntity::find()
            .filter(SinkColumn::ConnectorName.eq(connector_name))
            .one(db)
            .await
            .ok()
            .flatten()
            .map(|one| one.id)
            .unwrap_or(0)
    }

    /// # 分页查询死信
    pub async fn page(dto: DbzDeadLetterQueryDto) -> Result<Ro<PageRx<DbzDeadLetterVo>>, SvcError> {
        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;

        let mut condition = Condition::all()
            .add(DeadLetterColumn::ConnectorType.eq(i16::from(DeadLetterConnectorType::Sink)));
        if let Some(connector_type) = dto.connector_type {
            condition =
                condition.add(DeadLetterColumn::ConnectorType.eq(i16::from(connector_type)));
        }
        if let Some(connector_id) = dto.connector_id {
            condition = condition.add(DeadLetterColumn::ConnectorId.eq(i64::from(connector_id)));
        }
        if let Some(status) = dto.status {
            condition = condition.add(DeadLetterColumn::Status.eq(i16::from(status)));
        }
        if let Some(topic) = dto.topic.filter(|topic| !topic.trim().is_empty()) {
            condition = condition.add(DeadLetterColumn::Topic.eq(topic.trim()));
        }
        if let Some(category) = dto.failure_category {
            condition = condition.add(DeadLetterColumn::FailureCategory.eq(i16::from(category)));
        }
        if let Some(subtype) = dto.failure_subtype {
            condition = condition.add(DeadLetterColumn::FailureSubtype.eq(i16::from(subtype)));
        }
        if let Some(reason) = dto.reason.filter(|reason| !reason.trim().is_empty()) {
            condition = condition.add(DeadLetterColumn::Reason.contains(reason.trim()));
        }
        if let Some(start_ms) = dto.start_ms {
            condition = condition.add(DeadLetterColumn::LastSeenMs.gte(start_ms));
        }
        if let Some(end_ms) = dto.end_ms {
            condition = condition.add(DeadLetterColumn::LastSeenMs.lte(end_ms));
        }

        let page_num = dto._page.unwrap_or(U64(1));
        let page_size = U64(dto
            ._size
            .map(|size| size.value())
            .unwrap_or(20)
            .clamp(1, 200));

        let (page_num, total, models) = DbzDeadLetterDao::page_by_condition::<_, DeadLetterModel>(
            condition,
            &Some("last_seen_ms:desc".to_string()),
            page_num,
            page_size,
            db.as_ref(),
        )
        .await?;

        Ok(Ro::success("查询死信记录成功".to_string()).extra(Some(
            PageRx::builder()
                .page_num(page_num)
                .total(total)
                .list(models.into_iter().map(to_vo).collect())
                .build(),
        )))
    }

    /// # 待处理计数
    ///
    /// 不传连接器时按连接器分组返回全部，前端一次请求即可给每张卡片挂角标。
    pub async fn pending_count(
        dto: DbzDeadLetterCountDto,
    ) -> Result<Ro<Vec<DbzDeadLetterCountVo>>, SvcError> {
        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;

        let mut condition = Condition::all()
            .add(DeadLetterColumn::Status.eq(i16::from(DeadLetterStatus::Pending)))
            .add(DeadLetterColumn::ConnectorType.eq(i16::from(DeadLetterConnectorType::Sink)));
        if let Some(connector_type) = dto.connector_type {
            condition =
                condition.add(DeadLetterColumn::ConnectorType.eq(i16::from(connector_type)));
        }
        if let Some(connector_id) = dto.connector_id {
            condition = condition.add(DeadLetterColumn::ConnectorId.eq(i64::from(connector_id)));
        }

        let rows = DeadLetterEntity::find()
            .select_only()
            .column(DeadLetterColumn::ConnectorType)
            .column(DeadLetterColumn::ConnectorId)
            .expr_as(
                sea_orm::sea_query::Func::count(Expr::col(DeadLetterColumn::Id)),
                "count",
            )
            .filter(condition)
            .group_by(DeadLetterColumn::ConnectorType)
            .group_by(DeadLetterColumn::ConnectorId)
            .into_tuple::<(i16, i64, i64)>()
            .all(db.as_ref())
            .await
            .map_err(|e| anyhow!("统计待处理死信失败: {e}"))?;

        let list = rows
            .into_iter()
            .map(
                |(connector_type, connector_id, count)| DbzDeadLetterCountVo {
                    connector_type: connector_type.into(),
                    connector_id: U64::from(connector_id),
                    count: count.max(0) as u64,
                },
            )
            .collect();
        Ok(Ro::success("查询待处理死信数量成功".to_string()).extra(Some(list)))
    }

    /// # 批量重投
    ///
    /// 把报文重新生产回**源 topic**（写回 Kafka，让 Connect 再消费一次），不是绕过
    /// Connect 直接 POST 下游 HTTP 地址。成功转「已重投」，失败保持原状态并回写原因。
    ///
    /// `payload` 不传时按 DLQ 里的原始报文重投；传入修改后的报文时只允许单条，
    /// 且必须符合分类白名单。修改后的报文另存，原始报文不覆盖。
    pub async fn retry(
        dto: DbzDeadLetterRetryDto,
        operator_id: U64,
    ) -> Result<Ro<Vec<DbzDeadLetterRetryResultVo>>, SvcError> {
        if dto.ids.is_empty() {
            return Err(anyhow!("ids 不能为空").into());
        }
        if dto.payload.is_some() && dto.ids.len() > 1 {
            return Err(anyhow!("修改报文后重投只支持单条记录").into());
        }
        let edited_payload = match dto.payload.as_deref() {
            Some(text) => {
                Some(Self::parse_payload_text(text).map_err(|e| anyhow!("待投报文校验失败: {e}"))?)
            }
            None => None,
        };
        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;
        let operator = i64::from(operator_id);
        let now = now_ms() as i64;

        let mut results = Vec::with_capacity(dto.ids.len());
        for id in dto.ids {
            let result = Self::retry_one(
                db.as_ref(),
                id,
                operator,
                now,
                dto.remark.as_deref(),
                edited_payload.as_ref(),
            )
            .await
            .map_err(|e| anyhow!("重投死信<{}>失败: {e}", id.value()))?;
            results.push(result);
        }

        Ok(Ro::success("重投完成".to_string()).extra(Some(results)))
    }

    /// # 保存待投报文（不重投）
    ///
    /// 原始报文 `payload` 永不覆盖，这里只写 `pending_payload` 与编辑审计字段。
    /// 只有「异常分类为字段不合规」的记录允许调用。
    pub async fn edit_payload(
        dto: DbzDeadLetterEditPayloadDto,
        operator_id: U64,
    ) -> Result<Ro<DbzDeadLetterVo>, SvcError> {
        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;
        let operator = i64::from(operator_id);
        let now = now_ms() as i64;

        let model = DeadLetterEntity::find_by_id(i64::from(dto.id))
            .one(db.as_ref())
            .await
            .map_err(|e| anyhow!("查询死信记录失败: {e}"))?
            .ok_or_else(|| anyhow!("死信记录不存在"))?;
        if model.status == i16::from(DeadLetterStatus::ManualHandled) {
            return Err(anyhow!("该记录已标记「已人工处理」，如需修改请先撤销").into());
        }

        Self::ensure_editable(&model)?;
        let payload =
            Self::parse_payload_text(&dto.payload).map_err(|e| anyhow!("待投报文校验失败: {e}"))?;
        Self::validate_field_payload(&payload, None)
            .map_err(|e| SvcError::from(anyhow!("待投报文校验失败: {e}")))?;

        let mut active: DeadLetterActiveModel = model.into();
        active.pending_payload = Set(Some(payload));
        active.payload_edited_by = Set(Some(operator));
        active.payload_edited_ms = Set(Some(now));
        active.updater_id = Set(operator);
        active.update_ms = Set(now);
        let model = DbzDeadLetterDao::update(active, db.as_ref()).await?;

        Ok(Ro::success("待投报文已保存".to_string()).extra(Some(to_vo(model))))
    }

    /// # 批量废弃
    ///
    /// 只改状态与处理信息，不删行，保留追溯。注意 DLQ topic 里的原始记录仍在，
    /// 废弃只是 dbz 页面上的结论。
    pub async fn discard(
        dto: DbzDeadLetterDiscardDto,
        operator_id: U64,
    ) -> Result<Ro<()>, SvcError> {
        if dto.ids.is_empty() {
            return Err(anyhow!("ids 不能为空").into());
        }
        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;
        let operator = i64::from(operator_id);
        let now = now_ms() as i64;
        let remark = dto.remark.as_deref().map(|value| truncate(value, 500));
        let ids: Vec<i64> = dto.ids.into_iter().map(i64::from).collect();

        DeadLetterEntity::update_many()
            .col_expr(
                DeadLetterColumn::Status,
                Expr::value(i16::from(DeadLetterStatus::Discarded)),
            )
            .col_expr(DeadLetterColumn::HandledBy, Expr::value(operator))
            .col_expr(DeadLetterColumn::HandledMs, Expr::value(now))
            .col_expr(DeadLetterColumn::Remark, Expr::value(remark))
            .col_expr(DeadLetterColumn::UpdaterId, Expr::value(operator))
            .col_expr(DeadLetterColumn::UpdateMs, Expr::value(now))
            .filter(DeadLetterColumn::Id.is_in(ids.clone()))
            .filter(DeadLetterColumn::Status.ne(i16::from(DeadLetterStatus::Retried)))
            .exec(db.as_ref())
            .await
            .map_err(|e| anyhow!("更新死信状态失败: {e}"))?;

        Ok(Ro::success(format!("已废弃 {} 条死信", ids.len())))
    }

    /// # 批量标记「已人工处理」
    pub async fn mark_handled(
        dto: DbzDeadLetterMarkHandledDto,
        operator_id: U64,
    ) -> Result<Ro<()>, SvcError> {
        if dto.ids.is_empty() {
            return Err(anyhow!("ids 不能为空").into());
        }
        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;
        let operator = i64::from(operator_id);
        let now = now_ms() as i64;
        let remark = dto.remark.as_deref().map(|value| truncate(value, 500));
        let ids: Vec<i64> = dto.ids.into_iter().map(i64::from).collect();

        let updated = DeadLetterEntity::update_many()
            .col_expr(
                DeadLetterColumn::Status,
                Expr::value(i16::from(DeadLetterStatus::ManualHandled)),
            )
            .col_expr(DeadLetterColumn::HandledBy, Expr::value(operator))
            .col_expr(DeadLetterColumn::HandledMs, Expr::value(now))
            .col_expr(DeadLetterColumn::Remark, Expr::value(remark))
            .col_expr(DeadLetterColumn::UpdaterId, Expr::value(operator))
            .col_expr(DeadLetterColumn::UpdateMs, Expr::value(now))
            .filter(DeadLetterColumn::Id.is_in(ids.clone()))
            .filter(DeadLetterColumn::Status.eq(i16::from(DeadLetterStatus::Pending)))
            .exec(db.as_ref())
            .await
            .map_err(|e| anyhow!("标记已人工处理失败: {e}"))?;

        Ok(Ro::success(format!(
            "已标记 {} 条死信为人工处理（跳过 {} 条非待处理记录）",
            updated.rows_affected,
            ids.len() as u64 - updated.rows_affected.min(ids.len() as u64)
        )))
    }

    /// # 批量撤销「已人工处理」
    pub async fn unmark_handled(
        dto: DbzDeadLetterUnmarkHandledDto,
        operator_id: U64,
    ) -> Result<Ro<()>, SvcError> {
        if dto.ids.is_empty() {
            return Err(anyhow!("ids 不能为空").into());
        }
        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;
        let operator = i64::from(operator_id);
        let now = now_ms() as i64;
        let ids: Vec<i64> = dto.ids.into_iter().map(i64::from).collect();

        let updated = DeadLetterEntity::update_many()
            .col_expr(
                DeadLetterColumn::Status,
                Expr::value(i16::from(DeadLetterStatus::Pending)),
            )
            .col_expr(
                DeadLetterColumn::HandledBy,
                Expr::value(sea_orm::Value::BigInt(None)),
            )
            .col_expr(
                DeadLetterColumn::HandledMs,
                Expr::value(sea_orm::Value::BigInt(None)),
            )
            .col_expr(
                DeadLetterColumn::Remark,
                Expr::value(sea_orm::Value::String(None)),
            )
            .col_expr(DeadLetterColumn::UpdaterId, Expr::value(operator))
            .col_expr(DeadLetterColumn::UpdateMs, Expr::value(now))
            .filter(DeadLetterColumn::Id.is_in(ids.clone()))
            .filter(DeadLetterColumn::Status.eq(i16::from(DeadLetterStatus::ManualHandled)))
            .exec(db.as_ref())
            .await
            .map_err(|e| anyhow!("撤销人工处理失败: {e}"))?;

        Ok(Ro::success(format!(
            "已撤销 {} 条死信的人工处理结论",
            updated.rows_affected
        )))
    }

    /// # 单条重投（人工触发）
    ///
    /// 把报文写回源 topic，交给 Kafka Connect 重新走一遍完整的 sink 链路。
    /// 无论成败都回写 `last_retry_*`，原始 `payload` 始终不动。
    async fn retry_one(
        db: &DatabaseConnection,
        id: U64,
        operator: i64,
        now: i64,
        remark: Option<&str>,
        edited_payload: Option<&Value>,
    ) -> anyhow::Result<DbzDeadLetterRetryResultVo> {
        let Some(mut model) = DeadLetterEntity::find_by_id(i64::from(id)).one(db).await? else {
            return Ok(DbzDeadLetterRetryResultVo {
                id,
                success: false,
                message: "死信记录不存在".to_string(),
            });
        };

        if model.status == i16::from(DeadLetterStatus::Retried) {
            return Ok(DbzDeadLetterRetryResultVo {
                id,
                success: false,
                message: "该记录已重投成功，无需重复重投".to_string(),
            });
        }
        if model.status == i16::from(DeadLetterStatus::ManualHandled) {
            return Ok(DbzDeadLetterRetryResultVo {
                id,
                success: false,
                message: "该记录已标记「已人工处理」，如需重投请先撤销".to_string(),
            });
        }

        let mut payload_to_post = model.payload.clone();
        if let Some(payload) = edited_payload {
            Self::ensure_editable(&model)?;
            Self::validate_field_payload(payload, None)
                .map_err(|e| anyhow!("待投报文校验失败: {e}"))?;
            payload_to_post = payload.clone();
            model.pending_payload = Some(payload.clone());
            model.payload_edited_by = Some(operator);
            model.payload_edited_ms = Some(now);
        }

        let post_result = Self::produce_back(db, &model, &payload_to_post).await;
        let success = post_result.is_ok();
        let message = match &post_result {
            Ok(()) => "已写回源 topic，等待 Kafka Connect 重新消费".to_string(),
            Err(error) => error.clone(),
        };

        if success {
            model.status = i16::from(DeadLetterStatus::Retried);
            model.handled_by = Some(operator);
            model.handled_ms = Some(now);
            model.remark = remark.map(|value| truncate(value, 500));
        }
        model.last_retry_success = Some(success);
        model.last_retry_ms = Some(now);
        model.last_retry_message = Some(message.clone());
        model.updater_id = operator;
        model.update_ms = now;

        let active: DeadLetterActiveModel = model.into();
        DbzDeadLetterDao::update(active, db).await?;

        Ok(DbzDeadLetterRetryResultVo {
            id,
            success,
            message,
        })
    }

    /// # 把报文写回源 topic
    ///
    /// broker 归属由 `connector_id → sink → connect → broker` 推导，接口从不接受
    /// 调用方直接传地址。源 topic 可能同时被多条下游连接器消费，重投会让它们都
    /// 再收到一次 —— 这是写回原始 topic 的固有语义，页面与文档都会提示。
    async fn produce_back(
        db: &DatabaseConnection,
        model: &DeadLetterModel,
        payload: &Value,
    ) -> Result<(), String> {
        if model.topic.is_empty() || model.topic.ends_with(":no-context") {
            return Err("DLQ 记录缺少源 topic 上下文，无法确定重投目标".to_string());
        }
        if model.connector_id == 0 {
            return Err(
                "DLQ 记录未归属到下游连接器（连接器名未在本系统登记），无法确定重投目标集群"
                    .to_string(),
            );
        }
        let sink = SinkEntity::find()
            .filter(SinkColumn::Id.eq(model.connector_id))
            .one(db)
            .await
            .map_err(|e| format!("查询下游连接器失败: {e}"))?
            .ok_or_else(|| format!("下游连接器(id={})不存在", model.connector_id))?;
        let connect = ConnectEntity::find_by_id(sink.kafka_connect_id)
            .one(db)
            .await
            .map_err(|e| format!("查询 Kafka Connect 失败: {e}"))?
            .ok_or_else(|| "下游连接器所属的 Kafka Connect 不存在".to_string())?;
        let broker = BrokerEntity::find_by_id(connect.kafka_broker_id)
            .one(db)
            .await
            .map_err(|e| format!("查询 Kafka 集群失败: {e}"))?
            .ok_or_else(|| "Kafka Connect 所属的 Kafka 集群不存在".to_string())?;

        let conn = BrokerConn::from_model(&broker);
        let client = KafkaBrokerUtils::connect(&conn)
            .await
            .map_err(|e| format!("连接 Kafka 集群<{}>失败: {e}", broker.name))?;
        let partition = model.partition.unwrap_or(0);
        let partition_client = client
            .partition_client(&model.topic, partition, UnknownTopicHandling::Error)
            .await
            .map_err(|e| format!("定位源 topic<{}>分区<{partition}>失败: {e}", model.topic))?;
        let record = Record {
            key: Some(model.record_key.as_bytes().to_vec()),
            value: Some(payload_bytes(payload)),
            headers: BTreeMap::new(),
            timestamp: chrono::Utc::now(),
        };
        partition_client
            .produce(
                vec![record],
                rskafka::client::partition::Compression::default(),
            )
            .await
            .map_err(|e| format!("写回源 topic<{}>失败: {e}", model.topic))?;
        Ok(())
    }

    /// # 是否允许修改报文
    fn ensure_editable(model: &DeadLetterModel) -> anyhow::Result<()> {
        if Self::can_edit_payload(model.failure_category, model.failure_subtype) {
            Ok(())
        } else {
            Err(anyhow!("该死信的异常不属于字段不合规类，禁止修改报文"))
        }
    }

    /// 白名单判定：数据/格式类（1）且子类为字段不合规（1）
    fn can_edit_payload(failure_category: i16, failure_subtype: i16) -> bool {
        failure_category == i16::from(DeadLetterFailureCategory::Deterministic)
            && failure_subtype == i16::from(DeadLetterFailureSubtype::FieldValidation)
    }

    /// # 死信记录给人看的一句失败原因
    ///
    /// 只按 DLQ 异常分类归一出结论，绝不把原始堆栈塞进这一列。
    fn dead_letter_reason(failure_category: i16, failure_subtype: i16) -> String {
        if failure_category == i16::from(DeadLetterFailureCategory::Deterministic) {
            if failure_subtype == i16::from(DeadLetterFailureSubtype::FieldValidation) {
                "报文结构或字段不合规，请核对并修改报文后重投".to_string()
            } else {
                "Kafka Connect 无法转换或写入该记录，请核对数据与转换器配置".to_string()
            }
        } else if failure_category == i16::from(DeadLetterFailureCategory::Transient) {
            "下游投递失败且 Connect 重试耗尽，请确认目标服务恢复后重投".to_string()
        } else {
            "Kafka Connect 处理该记录时失败，请查看失败详情".to_string()
        }
    }

    /// 解析待投报文文本，错误信息直接可读
    fn parse_payload_text(text: &str) -> Result<Value, String> {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return Err("待投报文不能为空".to_string());
        }
        serde_json::from_str::<Value>(trimmed).map_err(|e| {
            format!(
                "不是合法 JSON：{e}（第 {} 行第 {} 列）",
                e.line(),
                e.column()
            )
        })
    }

    /// 待投报文必须是 JSON 对象；DLQ 记录若本来不是 JSON 文本，不允许改写
    fn validate_field_payload(payload: &Value, _config: Option<&Value>) -> Result<(), String> {
        if payload.get("__raw_utf8").is_some() {
            return Err("原始报文不是 JSON，不能修改后重投".to_string());
        }
        if !payload.is_object() {
            return Err("待投报文必须是 JSON 对象（{...}）".to_string());
        }
        Ok(())
    }

    /// # 保留清理
    ///
    /// 只清页面索引：DLQ topic 里的原始记录由 Kafka 的 retention 管理，dbz 不碰。
    async fn maybe_cleanup(db: &DatabaseConnection, cfg: &DeadLetterConfig) -> anyhow::Result<()> {
        static LAST_CLEANUP: LazyLock<Mutex<i64>> = LazyLock::new(|| Mutex::new(0));
        let now = now_ms() as i64;
        {
            let mut last = LAST_CLEANUP.lock().await;
            if now - *last < CLEANUP_INTERVAL_MS {
                return Ok(());
            }
            *last = now;
        }

        let cutoff = now - cfg.retention_days as i64 * 24 * 60 * 60 * 1_000;
        DeadLetterEntity::delete_many()
            .filter(DeadLetterColumn::LastSeenMs.lt(cutoff))
            .exec(db)
            .await?;

        // 单连接器上限：按最后失败时间保留最新的 N 条
        let sql = "DELETE FROM dbz_dead_letter WHERE id IN (
            SELECT id FROM (
                SELECT id, row_number() OVER (
                    PARTITION BY connector_type, connector_id
                    ORDER BY last_seen_ms DESC
                ) AS rn
                FROM dbz_dead_letter
            ) ranked WHERE ranked.rn > $1
        )";
        db.execute_raw(sea_orm::Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Postgres,
            sql,
            [sea_orm::Value::BigInt(Some(cfg.max_per_connector as i64))],
        ))
        .await?;
        Ok(())
    }
}

/// 读取当前死信配置；尚未加载配置时回落默认值
fn current_config() -> DeadLetterConfig {
    get_dbz_config()
        .map(|config| config.dead_letter.clone())
        .unwrap_or_default()
}

/// 模型转 VO
///
/// `Model` 与 `DbzDeadLetterVo` 都在 `dbz-api` 里，本 crate 不能为它们实现
/// `From`（孤儿规则），所以用本地函数。
fn to_vo(model: DeadLetterModel) -> DbzDeadLetterVo {
    DbzDeadLetterVo {
        id: U64::from(model.id),
        connector_type: model.connector_type.into(),
        connector_id: U64::from(model.connector_id),
        topic: model.topic,
        record_key: model.record_key,
        partition: model.partition,
        offset: model.offset,
        payload: model.payload,
        failure_category: model.failure_category.into(),
        failure_subtype: model.failure_subtype.into(),
        can_edit_payload: DbzDeadLetterSvc::can_edit_payload(
            model.failure_category,
            model.failure_subtype,
        ),
        pending_payload: model.pending_payload,
        payload_edited_by: model.payload_edited_by.map(U64::from),
        payload_edited_ms: model.payload_edited_ms,
        last_retry_success: model.last_retry_success,
        last_retry_ms: model.last_retry_ms,
        last_retry_message: model.last_retry_message,
        reason: model.reason,
        failure_detail: model.failure_detail,
        retry_count: model.retry_count.max(0) as u32,
        first_seen_ms: model.first_seen_ms,
        last_seen_ms: model.last_seen_ms,
        status: model.status.into(),
        handled_by: model.handled_by.map(U64::from),
        handled_ms: model.handled_ms,
        remark: model.remark,
        create_ms: model.create_ms,
        update_ms: model.update_ms,
    }
}

/// 按字符截断，避免超长字段撞库列长度
fn truncate(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

#[cfg(test)]
mod tests {
    use super::DbzDeadLetterSvc;
    use crate::dic::{DeadLetterFailureCategory, DeadLetterFailureSubtype};

    #[test]
    fn only_deterministic_field_validation_can_edit() {
        assert!(DbzDeadLetterSvc::can_edit_payload(
            i16::from(DeadLetterFailureCategory::Deterministic),
            i16::from(DeadLetterFailureSubtype::FieldValidation),
        ));
        assert!(!DbzDeadLetterSvc::can_edit_payload(
            i16::from(DeadLetterFailureCategory::Transient),
            i16::from(DeadLetterFailureSubtype::FieldValidation),
        ));
        assert!(!DbzDeadLetterSvc::can_edit_payload(
            i16::from(DeadLetterFailureCategory::Deterministic),
            i16::from(DeadLetterFailureSubtype::Other),
        ));
    }

    #[test]
    fn invalid_json_has_readable_error() {
        let error = DbzDeadLetterSvc::parse_payload_text("{oops").unwrap_err();
        assert!(error.contains("不是合法 JSON"));
    }

    #[test]
    fn empty_payload_rejected() {
        assert!(DbzDeadLetterSvc::parse_payload_text("  ").is_err());
    }

    #[test]
    fn payload_must_be_object() {
        let value = serde_json::json!([1, 2]);
        assert!(DbzDeadLetterSvc::validate_field_payload(&value, None).is_err());
    }

    #[test]
    fn raw_payload_cannot_be_edited() {
        let value = serde_json::json!({ "__raw_utf8": "id,name\n1,a" });
        assert!(DbzDeadLetterSvc::validate_field_payload(&value, None).is_err());
    }

    #[test]
    fn dead_letter_reason_is_human_readable_by_category() {
        let reason = DbzDeadLetterSvc::dead_letter_reason(
            i16::from(DeadLetterFailureCategory::Transient),
            i16::from(DeadLetterFailureSubtype::Other),
        );
        assert!(reason.contains("重试耗尽"));
        let reason = DbzDeadLetterSvc::dead_letter_reason(
            i16::from(DeadLetterFailureCategory::Deterministic),
            i16::from(DeadLetterFailureSubtype::FieldValidation),
        );
        assert!(reason.contains("字段不合规"));
    }
}
