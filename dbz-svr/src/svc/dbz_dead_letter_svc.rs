//! # 死信与重试队列服务
//!
//! ## 为什么重试与死信必须由 dbz 自己驱动
//!
//! HttpSink 的 `put` 只把 `IOException` / `InterruptedException` 视为可重试异常；
//! 对端返回 HTTP 4xx/5xx 时它**只记日志并照常提交偏移量**。magic 脚本的业务失败
//! 又是 HTTP 200 + `{"result":-2}`，Connect 视为成功。因此：
//!
//! - `errors.tolerance` / Connect DLQ 兜不住任何东西，偏移已经提交；
//! - 重试必须落在数据库队列里，由本服务的 worker 驱动；
//! - 死信只能由脚本显式上报。
//!
//! ## 三类失败
//!
//! | 分类 | 入口 | 处理 |
//! | --- | --- | --- |
//! | 确定性失败（参数、唯一键、外键） | 直接写死信表 | 不重试，等人工重投或废弃 |
//! | 传输类失败（连接失败、5xx、超时） | 写重试队列 | 1s→5s→30s→2m→10m，10 次后转死信 |
//! | 响应异常（result 非成功且无法归类 / 响应无法解析） | 写重试队列 | 同上，分类记 `Unknown` |
//!
//! ## 与 magic 侧旧表的关系
//!
//! `dtw同步到dtwrs.dtw_locator_bind` 已在 magic 侧建了 `dtwrs_bind_retry` 与
//! `dtwrs_bind_dead_letter`。本服务**不并行维护第二套终态**：这两个表作为迁移来源，
//! 脚本改为调用本服务的上报接口后停止写入；存量数据用一次性导入脚本搬到
//! `dbz_dead_letter`（保留同一幂等键）。保留在 magic 侧的理由只剩「脚本失败时
//! 的事务上下文」，而那是重试队列的写入动作，不是终态。
//!
//! 选择 dbz 侧的理由：连接器身份、启停状态、权限与页面都在 dbz；magic 侧表按脚本
//! 各建一套，控制面看不到、也无法统一保留与重投。重投目标地址从对应下游连接器的
//! `connector_config.url` 取，不需要脚本额外传参。

use crate::dao::{DbzDeadLetterDao, DbzDeadLetterRetryDao, DbzSinkConnectorDao};
use crate::dic::{DeadLetterConnectorType, DeadLetterFailureCategory, DeadLetterStatus};
use crate::dto::{
    DbzDeadLetterCountDto, DbzDeadLetterDiscardDto, DbzDeadLetterQueryDto, DbzDeadLetterReportDto,
    DbzDeadLetterRetryDto,
};
use crate::mo::dbz_dead_letter::{
    ActiveModel as DeadLetterActiveModel, Column as DeadLetterColumn, Entity as DeadLetterEntity,
    Model as DeadLetterModel,
};
use crate::mo::dbz_dead_letter_retry::{
    ActiveModel as RetryActiveModel, Column as RetryColumn, Entity as RetryEntity,
    Model as RetryModel,
};
use crate::mo::dbz_sink_connector::Model as SinkModel;
use crate::vo::{DbzDeadLetterCountVo, DbzDeadLetterRetryResultVo, DbzDeadLetterVo};
use anyhow::anyhow;
use robotech::api::{PageRx, Ro, U64};
use robotech::db::get_db_conn;
use robotech::svc::SvcError;
use sea_orm::sea_query::Expr;
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseConnection, EntityTrait, QueryFilter,
    QueryOrder, QuerySelect, Set,
};
use serde_json::Value;
use std::time::Duration;
use tracing::{info, warn};
use wheel_rs::time_utils::now_ms;

/// 传输类失败的重试间隔：1s → 5s → 30s → 2m → 10m
const RETRY_DELAYS_MS: [i64; 5] = [1_000, 5_000, 30_000, 120_000, 600_000];

/// 清理周期：10 分钟一次
const CLEANUP_INTERVAL_MS: i64 = 10 * 60 * 1_000;

/// # 死信服务
///
/// 没有对应的 CRUD 路由，只有上报、查询、重投、废弃、计数与后台 worker，
/// 因此不使用 `#[svc]` 宏。
pub struct DbzDeadLetterSvc;

impl DbzDeadLetterSvc {
    /// # 上报一条死信（供 magic 脚本调用）
    ///
    /// 确定性失败直接写死信表；传输类与响应异常写重试队列，由 worker 按退避重投。
    /// 已存在的重试行只累加次数、刷新时间，不新增行。
    ///
    /// ## 返回值
    /// * `Ok(Ro<()>)` - 已入死信或已加入重试队列
    /// * `Err(SvcError)` - 参数不合法或读写库失败
    pub async fn report(dto: DbzDeadLetterReportDto, operator_id: U64) -> Result<Ro<()>, SvcError> {
        let connector_id = i64::from(dto.connector_id);
        let connector_type = i16::from(dto.connector_type);
        let topic = dto.topic.trim().to_string();
        if topic.is_empty() {
            return Err(anyhow!("topic 不能为空").into());
        }
        if connector_id <= 0 {
            return Err(anyhow!("connectorId 必须为正数").into());
        }

        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;
        let now = now_ms() as i64;
        let record_key = truncate(&dto.record_key, 512);
        let failure_detail = truncate(&dto.failure_detail, 5_000);
        let attempts = dto.retry_count as i32;
        let operator = i64::from(operator_id);

        if dto.failure_category == DeadLetterFailureCategory::Deterministic {
            // 确定性失败不再等重试：若正排在队列里，直接撤掉
            Self::delete_retry(
                db.as_ref(),
                connector_type,
                connector_id,
                &topic,
                &record_key,
            )
            .await?;
            let _ = Self::upsert_dead_letter(
                db.as_ref(),
                connector_type,
                connector_id,
                &topic,
                &record_key,
                dto.payload,
                i16::from(dto.failure_category),
                &failure_detail,
                attempts + 1,
                now,
                now,
                operator,
            )
            .await?;
            return Ok(Ro::success("已写入死信".to_string()));
        }

        // 上游连接器没有可重投的 HTTP 地址：传输类失败进队列也重投不了，
        // 直接落死信，避免 worker 空转 10 次。
        if dto.connector_type == DeadLetterConnectorType::Source {
            Self::delete_retry(
                db.as_ref(),
                connector_type,
                connector_id,
                &topic,
                &record_key,
            )
            .await?;
            let _ = Self::upsert_dead_letter(
                db.as_ref(),
                connector_type,
                connector_id,
                &topic,
                &record_key,
                dto.payload,
                i16::from(dto.failure_category),
                &failure_detail,
                attempts + 1,
                now,
                now,
                operator,
            )
            .await?;
            return Ok(Ro::success("已写入死信（上游连接器不可重投）".to_string()));
        }

        // 传输类 / 响应异常：先进重试队列
        let _ = Self::upsert_retry(
            db.as_ref(),
            connector_type,
            connector_id,
            &topic,
            &record_key,
            dto.payload,
            i16::from(dto.failure_category),
            &failure_detail,
            attempts,
            now,
            now,
        )
        .await?;
        Ok(Ro::success("已加入重试队列".to_string()))
    }

    /// # 分页查询死信
    ///
    /// 只查死信终态表；重试队列中的记录还没「死」，不在这里出现。
    pub async fn page(dto: DbzDeadLetterQueryDto) -> Result<Ro<PageRx<DbzDeadLetterVo>>, SvcError> {
        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;

        let mut condition = Condition::all();
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
            &Some("last_seen_ms desc".to_string()),
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

        let mut condition =
            Condition::all().add(DeadLetterColumn::Status.eq(i16::from(DeadLetterStatus::Pending)));
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
    /// 逐条 POST 到下游连接器配置里的 `url`（magic 的 sync 接口），HTTP 2xx 且
    /// 响应 `result=1` 才算成功；成功转「已重投」，失败保持「待处理」并返回原因。
    /// 单条重投就是只传一个 ID。
    pub async fn retry(
        dto: DbzDeadLetterRetryDto,
        operator_id: U64,
    ) -> Result<Ro<Vec<DbzDeadLetterRetryResultVo>>, SvcError> {
        if dto.ids.is_empty() {
            return Err(anyhow!("ids 不能为空").into());
        }
        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;
        let cfg = crate::config::get_dbz_config()
            .map_err(|e| anyhow!("读取 dbz 配置失败: {e}"))?
            .dead_letter
            .clone();
        let operator = i64::from(operator_id);
        let now = now_ms() as i64;

        let mut results = Vec::with_capacity(dto.ids.len());
        for id in dto.ids {
            let result = Self::retry_one(
                db.as_ref(),
                id,
                operator,
                now,
                cfg.request_timeout_ms,
                dto.remark.as_deref(),
            )
            .await
            .map_err(|e| anyhow!("重投死信<{}>失败: {e}", id.value()))?;
            results.push(result);
        }

        Ok(Ro::success("重投完成".to_string()).extra(Some(results)))
    }

    /// # 批量废弃
    ///
    /// 只改状态与处理信息，不删行，保留追溯。
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

    /// # 后台重试 worker
    ///
    /// 在 `main::setup` 首次加载配置时 spawn 一次。每轮重新读取配置，支持热更新
    /// 开关与间隔；到期行先用「把 next_at 推到 claim 超时之后」原子占位，避免多实例
    /// 重复重投。
    pub async fn run_retry_worker() {
        let mut last_cleanup = 0i64;
        loop {
            let cfg = match crate::config::get_dbz_config() {
                Ok(config) => config.dead_letter.clone(),
                Err(e) => {
                    warn!("读取 dbz 配置失败，死信 worker 使用默认值: {e}");
                    crate::config::DeadLetterConfig::default()
                }
            };

            if cfg.enabled {
                if let Err(e) = Self::retry_due(&cfg).await {
                    warn!("死信重试 worker 执行失败: {e}");
                }
                let now = now_ms() as i64;
                if now - last_cleanup >= CLEANUP_INTERVAL_MS {
                    if let Err(e) = Self::cleanup(&cfg).await {
                        warn!("死信保留清理失败: {e}");
                    }
                    last_cleanup = now;
                }
            }

            tokio::time::sleep(Duration::from_millis(cfg.worker_interval_ms.max(200))).await;
        }
    }

    /// # 处理一批到期的重试行
    async fn retry_due(cfg: &crate::config::DeadLetterConfig) -> anyhow::Result<()> {
        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;
        let now = now_ms() as i64;

        let due = RetryEntity::find()
            .filter(RetryColumn::NextAtMs.lte(now))
            .order_by_asc(RetryColumn::NextAtMs)
            .limit(cfg.batch_size.max(1))
            .all(db.as_ref())
            .await?;

        for row in due {
            // 原子占位：只有 next_at 仍到期的那一个实例能拿到
            let claimed = RetryEntity::update_many()
                .col_expr(
                    RetryColumn::NextAtMs,
                    Expr::value(now + cfg.request_timeout_ms as i64),
                )
                .col_expr(RetryColumn::UpdateMs, Expr::value(now))
                .filter(RetryColumn::Id.eq(row.id))
                .filter(RetryColumn::NextAtMs.lte(now))
                .exec(db.as_ref())
                .await?;
            if claimed.rows_affected == 0 {
                continue;
            }

            Self::attempt_retry(db.as_ref(), row, cfg, now).await?;
        }
        Ok(())
    }

    /// # 对一行执行一次重投
    async fn attempt_retry(
        db: &DatabaseConnection,
        row: RetryModel,
        cfg: &crate::config::DeadLetterConfig,
        now: i64,
    ) -> anyhow::Result<()> {
        let attempts = row.attempts + 1;
        let is_sink = row.connector_type == i16::from(DeadLetterConnectorType::Sink);
        let post_result = if is_sink {
            match Self::target_url(db, row.connector_type, row.connector_id).await {
                Ok(url) => Self::post_to_magic(&url, &row.payload, cfg.request_timeout_ms).await,
                Err(e) => Err(e),
            }
        } else {
            Err("上游连接器没有可重投的 HTTP 地址".to_string())
        };

        match post_result {
            Ok(()) => {
                info!(
                    "死信重投成功: connector={}(id={}) topic={} key={}",
                    row.connector_type, row.connector_id, row.topic, row.record_key
                );
                RetryEntity::delete_by_id(row.id).exec(db).await?;
            }
            Err(error) => {
                let extra = if is_sink {
                    ""
                } else {
                    "（上游连接器没有可重投的 HTTP 地址，直接转死信）"
                };
                if !is_sink || attempts >= cfg.max_attempts as i32 {
                    // 重试耗尽：转死信，不再保留队列行
                    Self::upsert_dead_letter(
                        db,
                        row.connector_type,
                        row.connector_id,
                        &row.topic,
                        &row.record_key,
                        row.payload.clone(),
                        row.failure_category,
                        &truncate(&format!("{error}{extra}"), 5_000),
                        attempts,
                        row.first_seen_ms,
                        now,
                        0,
                    )
                    .await?;
                    RetryEntity::delete_by_id(row.id).exec(db).await?;
                    warn!(
                        "死信重试耗尽({attempts}次)，已转死信: connector={}(id={}) topic={}: {error}",
                        row.connector_type, row.connector_id, row.topic
                    );
                } else {
                    let next_at = now + retry_delay_ms(attempts);
                    let mut active: RetryActiveModel = row.into();
                    active.attempts = Set(attempts);
                    active.next_at_ms = Set(next_at);
                    active.last_error = Set(Some(truncate(&format!("{error}{extra}"), 5_000)));
                    active.last_seen_ms = Set(now);
                    active.update_ms = Set(now);
                    DbzDeadLetterRetryDao::update(active, db).await?;
                    info!(
                        "死信重投失败，{attempts}/{} 次，{}ms 后重试: {error}{extra}",
                        cfg.max_attempts,
                        next_at - now
                    );
                }
            }
        }
        Ok(())
    }

    /// # 单条重投（人工触发）
    async fn retry_one(
        db: &DatabaseConnection,
        id: U64,
        operator: i64,
        now: i64,
        timeout_ms: u64,
        remark: Option<&str>,
    ) -> anyhow::Result<DbzDeadLetterRetryResultVo> {
        let Some(model) = DeadLetterEntity::find_by_id(i64::from(id)).one(db).await? else {
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

        let url = match Self::target_url(db, model.connector_type, model.connector_id).await {
            Ok(url) => url,
            Err(e) => {
                return Ok(DbzDeadLetterRetryResultVo {
                    id,
                    success: false,
                    message: e,
                });
            }
        };

        match Self::post_to_magic(&url, &model.payload, timeout_ms).await {
            Ok(()) => {
                let mut active: DeadLetterActiveModel = model.into();
                active.status = Set(i16::from(DeadLetterStatus::Retried));
                active.handled_by = Set(Some(operator));
                active.handled_ms = Set(Some(now));
                active.remark = Set(remark.map(|value| truncate(value, 500)));
                active.updater_id = Set(operator);
                active.update_ms = Set(now);
                DbzDeadLetterDao::update(active, db).await?;
                Ok(DbzDeadLetterRetryResultVo {
                    id,
                    success: true,
                    message: "重投成功".to_string(),
                })
            }
            Err(error) => Ok(DbzDeadLetterRetryResultVo {
                id,
                success: false,
                message: error,
            }),
        }
    }

    /// # 取重投目标地址
    ///
    /// 下游连接器的 `connector_config` 里 `url` 就是 HttpSink 投递的 magic 脚本
    /// 地址，重投即把原报文再 POST 一次。上游连接器没有这个地址，直接报错。
    async fn target_url(
        db: &DatabaseConnection,
        connector_type: i16,
        connector_id: i64,
    ) -> Result<String, String> {
        if connector_type != i16::from(DeadLetterConnectorType::Sink) {
            return Err("上游连接器没有可重投的 HTTP 地址".to_string());
        }
        let record = DbzSinkConnectorDao::get_by_id::<_, SinkModel>(U64::from(connector_id), db)
            .await
            .map_err(|e| format!("查询下游连接器失败: {e}"))?
            .ok_or_else(|| format!("下游连接器(id={connector_id})不存在"))?;
        if !record.enabled {
            return Err(format!("下游连接器<{}>已停用，不能重投", record.name));
        }

        let config: Value = serde_json::from_str(&record.connector_config)
            .map_err(|e| format!("解析连接器<{}>配置失败: {e}", record.name))?;
        config
            .get("url")
            .or_else(|| config.get("http.url"))
            .and_then(Value::as_str)
            .map(|url| url.to_string())
            .ok_or_else(|| format!("连接器<{}>配置缺少 url，无法重投", record.name))
    }

    /// # POST 原报文到 magic
    ///
    /// magic 脚本的成功约定是 `result=1`；`{"result":-2}` 等一律视为失败。
    async fn post_to_magic(url: &str, payload: &Value, timeout_ms: u64) -> Result<(), String> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()
            .map_err(|e| format!("创建 HTTP 客户端失败: {e}"))?;
        let resp = client
            .post(url)
            .json(payload)
            .send()
            .await
            .map_err(|e| format!("重投请求失败: {e}"))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(format!(
                "magic 返回 HTTP {status}: {}",
                truncate(&body, 500)
            ));
        }
        let value: Value = serde_json::from_str(&body)
            .map_err(|e| format!("响应不是 JSON: {e}; body={}", truncate(&body, 500)))?;
        let result = value.get("result").and_then(Value::as_i64);
        if result == Some(1) {
            Ok(())
        } else {
            Err(format!(
                "magic 返回 result={result:?}: {}",
                truncate(&body, 500)
            ))
        }
    }

    /// # 写重试队列（按幂等键 upsert）
    #[allow(clippy::too_many_arguments)]
    async fn upsert_retry(
        db: &DatabaseConnection,
        connector_type: i16,
        connector_id: i64,
        topic: &str,
        record_key: &str,
        payload: Value,
        failure_category: i16,
        failure_detail: &str,
        attempts: i32,
        first_seen_ms: i64,
        last_seen_ms: i64,
    ) -> anyhow::Result<RetryModel> {
        let condition = Self::retry_key_condition(connector_type, connector_id, topic, record_key);
        let now = now_ms() as i64;

        if let Some(existing) = RetryEntity::find()
            .filter(condition.clone())
            .one(db)
            .await?
        {
            let next_attempts = existing.attempts + attempts.max(0);
            let mut active: RetryActiveModel = existing.into();
            active.payload = Set(payload);
            active.failure_category = Set(failure_category);
            active.failure_detail = Set(failure_detail.to_string());
            active.attempts = Set(next_attempts);
            active.next_at_ms = Set(now + retry_delay_ms(next_attempts));
            active.last_seen_ms = Set(last_seen_ms);
            active.update_ms = Set(now);
            return Ok(DbzDeadLetterRetryDao::update(active, db).await?);
        }

        let active = RetryActiveModel {
            connector_type: Set(connector_type),
            connector_id: Set(connector_id),
            topic: Set(topic.to_string()),
            record_key: Set(record_key.to_string()),
            payload: Set(payload),
            failure_category: Set(failure_category),
            failure_detail: Set(failure_detail.to_string()),
            attempts: Set(attempts.max(0)),
            next_at_ms: Set(now + retry_delay_ms(attempts.max(0))),
            last_error: Set(None),
            first_seen_ms: Set(first_seen_ms),
            last_seen_ms: Set(last_seen_ms),
            create_ms: Set(now),
            update_ms: Set(now),
            ..Default::default()
        };
        Ok(DbzDeadLetterRetryDao::insert(active, db).await?)
    }

    /// # 写死信终态（按幂等键 upsert）
    ///
    /// 已存在时累加失败次数、刷新末次时间、重置为「待处理」并清空上一轮处理信息；
    /// 这条记录又被人工重投后再次失败，说明上一轮处理结论已失效。
    #[allow(clippy::too_many_arguments)]
    async fn upsert_dead_letter(
        db: &DatabaseConnection,
        connector_type: i16,
        connector_id: i64,
        topic: &str,
        record_key: &str,
        payload: Value,
        failure_category: i16,
        failure_detail: &str,
        increment: i32,
        first_seen_ms: i64,
        last_seen_ms: i64,
        operator: i64,
    ) -> anyhow::Result<DeadLetterModel> {
        let condition = Self::key_condition(connector_type, connector_id, topic, record_key);
        let now = now_ms() as i64;

        if let Some(existing) = DeadLetterEntity::find()
            .filter(condition.clone())
            .one(db)
            .await?
        {
            let next_retry_count = existing.retry_count + increment.max(0);
            let first_seen = first_seen_ms.min(existing.first_seen_ms);
            let mut active: DeadLetterActiveModel = existing.into();
            active.payload = Set(payload);
            active.failure_category = Set(failure_category);
            active.failure_detail = Set(failure_detail.to_string());
            active.retry_count = Set(next_retry_count);
            active.first_seen_ms = Set(first_seen);
            active.last_seen_ms = Set(last_seen_ms);
            active.status = Set(i16::from(DeadLetterStatus::Pending));
            active.handled_by = Set(None);
            active.handled_ms = Set(None);
            active.remark = Set(None);
            active.updater_id = Set(operator);
            active.update_ms = Set(now);
            return Ok(DbzDeadLetterDao::update(active, db).await?);
        }

        let active = DeadLetterActiveModel {
            connector_type: Set(connector_type),
            connector_id: Set(connector_id),
            topic: Set(topic.to_string()),
            record_key: Set(record_key.to_string()),
            payload: Set(payload),
            failure_category: Set(failure_category),
            failure_detail: Set(failure_detail.to_string()),
            retry_count: Set(increment.max(0)),
            first_seen_ms: Set(first_seen_ms),
            last_seen_ms: Set(last_seen_ms),
            status: Set(i16::from(DeadLetterStatus::Pending)),
            handled_by: Set(None),
            handled_ms: Set(None),
            remark: Set(None),
            creater_id: Set(operator),
            create_ms: Set(now),
            updater_id: Set(operator),
            update_ms: Set(now),
            ..Default::default()
        };
        Ok(DbzDeadLetterDao::insert(active, db).await?)
    }

    /// 幂等键条件（死信表）
    fn key_condition(
        connector_type: i16,
        connector_id: i64,
        topic: &str,
        record_key: &str,
    ) -> Condition {
        Condition::all()
            .add(DeadLetterColumn::ConnectorType.eq(connector_type))
            .add(DeadLetterColumn::ConnectorId.eq(connector_id))
            .add(DeadLetterColumn::Topic.eq(topic))
            .add(DeadLetterColumn::RecordKey.eq(record_key))
    }

    /// 幂等键条件（重试队列）
    fn retry_key_condition(
        connector_type: i16,
        connector_id: i64,
        topic: &str,
        record_key: &str,
    ) -> Condition {
        Condition::all()
            .add(RetryColumn::ConnectorType.eq(connector_type))
            .add(RetryColumn::ConnectorId.eq(connector_id))
            .add(RetryColumn::Topic.eq(topic))
            .add(RetryColumn::RecordKey.eq(record_key))
    }

    /// 删掉同一幂等键的重试行（确定性失败时用）
    async fn delete_retry(
        db: &DatabaseConnection,
        connector_type: i16,
        connector_id: i64,
        topic: &str,
        record_key: &str,
    ) -> anyhow::Result<()> {
        RetryEntity::delete_many()
            .filter(
                Condition::all()
                    .add(RetryColumn::ConnectorType.eq(connector_type))
                    .add(RetryColumn::ConnectorId.eq(connector_id))
                    .add(RetryColumn::Topic.eq(topic))
                    .add(RetryColumn::RecordKey.eq(record_key)),
            )
            .exec(db)
            .await?;
        Ok(())
    }

    /// # 保留清理
    ///
    /// 死信：超过保留天数按 `last_seen_ms` 删除；单个连接器超过上限时淘汰最旧的。
    /// 重试队列：超过保留天数的行也清掉，避免地址长期失效的记录一直轮询。
    async fn cleanup(cfg: &crate::config::DeadLetterConfig) -> anyhow::Result<()> {
        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;
        let now = now_ms() as i64;
        let cutoff = now - cfg.retention_days as i64 * 24 * 60 * 60 * 1_000;

        DeadLetterEntity::delete_many()
            .filter(DeadLetterColumn::LastSeenMs.lt(cutoff))
            .exec(db.as_ref())
            .await?;
        RetryEntity::delete_many()
            .filter(RetryColumn::LastSeenMs.lt(cutoff))
            .exec(db.as_ref())
            .await?;

        // 单连接器上限：按最后一次失败时间保留最新的 N 条
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
        payload: model.payload,
        failure_category: model.failure_category.into(),
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

/// 第 `attempts` 次失败后的等待间隔
fn retry_delay_ms(attempts: i32) -> i64 {
    let index = (attempts.max(0) as usize).min(RETRY_DELAYS_MS.len() - 1);
    RETRY_DELAYS_MS[index]
}
