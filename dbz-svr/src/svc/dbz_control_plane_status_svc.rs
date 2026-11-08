//! # 控制面状态刷新服务
//!
//! 控制面只展示一个统一状态（[`ConnectorRunStatus`]）。进入页面时前端异步调用
//! 本接口刷新一次，把状态回写到 `dbz_source_connector` / `dbz_sink_connector`
//! 的 `run_status` 与 `run_msg`；健康检查任务也用同一套枚举，两处口径一致。
//!
//! 刷新只读不写集群：连接器不存在时记「仅纳管未部署」，绝不顺手部署；已停用的
//! 记录不发实时请求，直接回写「已停用」；Connect 连不上时回写「不可达」，并在
//! `run_msg` 里保留最后已知状态与时间。

use crate::dao::{DbzSinkConnectorDao, DbzSourceConnectorDao};
use crate::dic::{ConnectorRunStatus, ControlPlaneNodeType};
use crate::dto::DbzResetConnectorOffsetsDto;
use crate::mo::dbz_kafka_connect::Entity as ConnectEntity;
use crate::mo::dbz_sink_connector::{
    ActiveModel as SinkActiveModel, Column as SinkColumn, Entity as SinkEntity, Model as SinkModel,
};
use crate::mo::dbz_source_connector::{
    ActiveModel as SourceActiveModel, Column as SourceColumn, Entity as SourceEntity,
    Model as SourceModel,
};
use crate::utils::{KafkaConnectorError, KafkaConnectorUtils};
use crate::vo::{DbzResetConnectorOffsetsVo, DbzResetOffsetsStepVo};
use anyhow::anyhow;
use chrono::{DateTime, FixedOffset};
use robotech::api::Ro;
use robotech::db::get_db_conn;
use robotech::svc::SvcError;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, Set};
use tracing::warn;
use wheel_rs::time_utils::now_ms;

/// # 控制面状态服务
pub struct DbzControlPlaneStatusSvc;

impl DbzControlPlaneStatusSvc {
    /// # 刷新全部连接器的统一状态
    ///
    /// 返回本次检查/回写的连接器数量。单个连接器或单台 Connect 失败只影响它自己，
    /// 不中断整轮刷新。
    pub async fn refresh() -> Result<Ro<u64>, SvcError> {
        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;
        let now = now_ms() as i64;
        let mut refreshed = 0_u64;

        let connects = ConnectEntity::find()
            .all(db.as_ref())
            .await
            .map_err(|e| anyhow!("读取 Kafka Connect 失败: {e}"))?;
        let connect_ids: std::collections::HashSet<i64> =
            connects.iter().map(|connect| connect.id).collect();
        // 所有上游连接器：用于发现下游「所属 Connect 与上游不一致」的旧记录
        let all_sources = SourceEntity::find()
            .all(db.as_ref())
            .await
            .map_err(|e| anyhow!("读取上游连接器失败: {e}"))?;
        let source_map: std::collections::HashMap<i64, i64> = all_sources
            .iter()
            .map(|source| (source.id, source.kafka_connect_id))
            .collect();

        for connect in connects {
            let connect_id = connect.id;
            let sources = SourceEntity::find()
                .filter(SourceColumn::KafkaConnectId.eq(connect_id))
                .all(db.as_ref())
                .await
                .map_err(|e| anyhow!("读取上游连接器失败: {e}"))?;
            let sinks = SinkEntity::find()
                .filter(SinkColumn::KafkaConnectId.eq(connect_id))
                .all(db.as_ref())
                .await
                .map_err(|e| anyhow!("读取下游连接器失败: {e}"))?;

            // 已停用的 Connect：名下连接器全部标为已停用，不发任何实时请求
            if !connect.enabled {
                for source in sources {
                    let msg = source
                        .enabled
                        .then(|| "所属 Kafka Connect 已停用".to_string());
                    Self::write_source(
                        db.as_ref(),
                        &source,
                        ConnectorRunStatus::Disabled,
                        msg,
                        now,
                    )
                    .await?;
                    refreshed += 1;
                }
                for sink in sinks {
                    let msg = sink
                        .enabled
                        .then(|| "所属 Kafka Connect 已停用".to_string());
                    Self::write_sink(db.as_ref(), &sink, ConnectorRunStatus::Disabled, msg, now)
                        .await?;
                    refreshed += 1;
                }
                continue;
            }

            // Connect 自身不可达：启用的连接器标不可达并保留最后已知状态；已停用的
            // 连接器仍然直接是「已停用」，不发任何请求
            if let Err(error) = KafkaConnectorUtils::check_connect(&connect.url).await {
                let cause = format!("Kafka Connect 不可达：{error}");
                for source in sources {
                    if !source.enabled {
                        Self::write_source(
                            db.as_ref(),
                            &source,
                            ConnectorRunStatus::Disabled,
                            None,
                            now,
                        )
                        .await?;
                        refreshed += 1;
                        continue;
                    }
                    let (status, msg) = Self::unreachable(&source, &cause);
                    Self::write_source(db.as_ref(), &source, status, msg, now).await?;
                    refreshed += 1;
                }
                for sink in sinks {
                    if !sink.enabled {
                        Self::write_sink(
                            db.as_ref(),
                            &sink,
                            ConnectorRunStatus::Disabled,
                            None,
                            now,
                        )
                        .await?;
                        refreshed += 1;
                        continue;
                    }
                    let (status, msg) = Self::unreachable(&sink, &cause);
                    Self::write_sink(db.as_ref(), &sink, status, msg, now).await?;
                    refreshed += 1;
                }
                continue;
            }

            for source in sources {
                let (status, msg) = Self::probe(
                    &connect.url,
                    &source.connector_name,
                    source.enabled,
                    &source,
                )
                .await;
                Self::write_source(db.as_ref(), &source, status, msg, now).await?;
                refreshed += 1;
            }
            for sink in sinks {
                // 已停用优先：不发实时请求，也不参与归属比对
                if !sink.enabled {
                    Self::write_sink(db.as_ref(), &sink, ConnectorRunStatus::Disabled, None, now)
                        .await?;
                    refreshed += 1;
                    continue;
                }
                // 归属不一致的旧记录统一落为失败，状态标签不再另画「不一致」
                let mismatch = source_map
                    .get(&sink.source_connector_id)
                    .map(|source_connect| *source_connect != sink.kafka_connect_id)
                    .unwrap_or(true);
                if mismatch {
                    Self::write_sink(
                        db.as_ref(),
                        &sink,
                        ConnectorRunStatus::Failed,
                        Some(
                            "所属 Connect 与上游连接器不一致，请编辑下游连接器重新选择上游"
                                .to_string(),
                        ),
                        now,
                    )
                    .await?;
                    refreshed += 1;
                    continue;
                }
                let (status, msg) =
                    Self::probe(&connect.url, &sink.connector_name, sink.enabled, &sink).await;
                Self::write_sink(db.as_ref(), &sink, status, msg, now).await?;
                refreshed += 1;
            }
        }

        // 所属 Connect 已被外部删掉的连接器：没有可探测的目标，统一标为不可达
        let orphan_sources = SourceEntity::find()
            .all(db.as_ref())
            .await
            .map_err(|e| anyhow!("读取上游连接器失败: {e}"))?;
        for source in orphan_sources {
            if connect_ids.contains(&source.kafka_connect_id) {
                continue;
            }
            let (status, msg) = Self::unreachable(&source, "所属 Kafka Connect 已不存在");
            Self::write_source(db.as_ref(), &source, status, msg, now).await?;
            refreshed += 1;
        }
        let orphan_sinks = SinkEntity::find()
            .all(db.as_ref())
            .await
            .map_err(|e| anyhow!("读取下游连接器失败: {e}"))?;
        for sink in orphan_sinks {
            if connect_ids.contains(&sink.kafka_connect_id) {
                continue;
            }
            if !sink.enabled {
                Self::write_sink(db.as_ref(), &sink, ConnectorRunStatus::Disabled, None, now)
                    .await?;
            } else {
                let (status, msg) = Self::unreachable(&sink, "所属 Kafka Connect 已不存在");
                Self::write_sink(db.as_ref(), &sink, status, msg, now).await?;
            }
            refreshed += 1;
        }

        Ok(Ro::success(format!("已刷新 {refreshed} 个连接器状态")).extra(Some(refreshed)))
    }

    /// # 探测单个连接器
    ///
    /// 已停用不请求；不存在记「未部署」；查询失败记「不可达」；其余按连接器与任务
    /// 状态归一到统一枚举。
    async fn probe<T: ConnectorRow>(
        base_url: &str,
        connector_name: &str,
        enabled: bool,
        record: &T,
    ) -> (ConnectorRunStatus, Option<String>) {
        if !enabled {
            return (ConnectorRunStatus::Disabled, None);
        }
        match KafkaConnectorUtils::fetch_connector_status(base_url, connector_name).await {
            Ok(health) => match health.state {
                crate::dic::KafkaConnectorStatus::Running => (ConnectorRunStatus::Running, None),
                crate::dic::KafkaConnectorStatus::Paused
                | crate::dic::KafkaConnectorStatus::Stopped => (
                    ConnectorRunStatus::Stopped,
                    Some(health.reason.unwrap_or_else(|| {
                        "连接器在 Kafka Connect 上为 PAUSED/STOPPED".to_string()
                    })),
                ),
                crate::dic::KafkaConnectorStatus::Failed => (
                    ConnectorRunStatus::Failed,
                    Some(
                        health
                            .reason
                            .unwrap_or_else(|| "连接器或任务处于 FAILED".to_string()),
                    ),
                ),
                state => (
                    ConnectorRunStatus::Failed,
                    Some(format!("连接器状态异常: {state}")),
                ),
            },
            Err(KafkaConnectorError::NotFound) => (
                ConnectorRunStatus::NotDeployed,
                Some("库内已启用，但 Kafka Connect 上不存在该连接器".to_string()),
            ),
            Err(error) => {
                let cause = format!("查询连接器状态失败：{error}");
                Self::unreachable(record, &cause)
            }
        }
    }

    /// # 不可达：保留最后已知状态与时间
    fn unreachable<T: ConnectorRow>(
        record: &T,
        cause: &str,
    ) -> (ConnectorRunStatus, Option<String>) {
        let known = record.run_status();
        // 连续不可达时保留上一次写下的「最后已知」，不要用新的「不可达」覆盖掉
        if known == 5 {
            return (
                ConnectorRunStatus::Unreachable,
                record.run_msg().map(str::to_string),
            );
        }
        let msg = format!(
            "{cause}；最后已知状态：{}（{}）",
            Self::status_label(known),
            Self::ms_text(record.update_ms())
        );
        (ConnectorRunStatus::Unreachable, Some(msg))
    }

    fn status_label(status: i16) -> &'static str {
        match status {
            1 => "运行中",
            2 => "已停用",
            3 => "失败",
            4 => "仅纳管未部署",
            5 => "不可达",
            6 => "已停止",
            _ => "未知",
        }
    }

    fn ms_text(ms: i64) -> String {
        DateTime::from_timestamp_millis(ms)
            .map(|time| {
                time.with_timezone(&FixedOffset::east_opt(8 * 3600).expect("东八区偏移固定合法"))
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string()
            })
            .unwrap_or_else(|| ms.to_string())
    }

    /// # 重置位点
    ///
    /// 按 Kafka Connect 的约束串行执行：停止连接器 → 删除位点 → （可选）恢复运行。
    /// 任一步失败立即停下，返回每步结果与停下时连接器的实际状态；下游连接器必须
    /// 由调用方显式确认「会重放历史消息」。
    pub async fn reset_offsets(
        dto: DbzResetConnectorOffsetsDto,
    ) -> Result<Ro<DbzResetConnectorOffsetsVo>, SvcError> {
        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;
        let is_sink = match dto.connector_type {
            ControlPlaneNodeType::Source => false,
            ControlPlaneNodeType::Sink => true,
            ControlPlaneNodeType::Broker | ControlPlaneNodeType::Connect => {
                return Err(anyhow!("只有上游/下游连接器可以重置位点").into());
            }
        };
        if is_sink && !dto.confirm_replay {
            return Ok(
                Ro::success("已取消：下游连接器需要确认会重放历史消息".to_string()).extra(Some(
                    DbzResetConnectorOffsetsVo {
                        ok: false,
                        failed_step: Some("confirm".to_string()),
                        message: "未确认会重复写入下游，已取消执行".to_string(),
                        connector_state: "未执行".to_string(),
                        steps: vec![],
                    },
                )),
            );
        }

        let connector_id = dto.connector_id.0 as i64;
        let (connect_id, connector_name) = if is_sink {
            let Some(sink) = SinkEntity::find_by_id(connector_id)
                .one(db.as_ref())
                .await
                .map_err(|e| anyhow!("读取下游连接器失败: {e}"))?
            else {
                return Err(anyhow!("下游连接器<{connector_id}>不存在").into());
            };
            (sink.kafka_connect_id, sink.connector_name)
        } else {
            let Some(source) = SourceEntity::find_by_id(connector_id)
                .one(db.as_ref())
                .await
                .map_err(|e| anyhow!("读取上游连接器失败: {e}"))?
            else {
                return Err(anyhow!("上游连接器<{connector_id}>不存在").into());
            };
            (source.kafka_connect_id, source.connector_name)
        };

        let Some(connect) = ConnectEntity::find_by_id(connect_id)
            .one(db.as_ref())
            .await
            .map_err(|e| anyhow!("读取 Kafka Connect 失败: {e}"))?
        else {
            return Err(anyhow!("Kafka Connect<{connect_id}>不存在").into());
        };
        if !connect.enabled {
            return Ok(
                Ro::success("所属 Kafka Connect 已停用".to_string()).extra(Some(
                    DbzResetConnectorOffsetsVo {
                        ok: false,
                        failed_step: Some("stop".to_string()),
                        message: "所属 Kafka Connect 已停用，无法执行".to_string(),
                        connector_state: "未执行".to_string(),
                        steps: vec![],
                    },
                )),
            );
        }

        let mut steps = Vec::new();

        // 1. 停止
        if let Err(error) = KafkaConnectorUtils::stop_connector(&connect.url, &connector_name).await
        {
            let state = Self::current_state(&connect.url, &connector_name).await;
            steps.push(DbzResetOffsetsStepVo {
                step: "stop".to_string(),
                ok: false,
                message: Some(error.to_string()),
            });
            let message = format!("停止连接器失败：{error}；当前状态：{state}");
            return Ok(
                Ro::success(message.clone()).extra(Some(DbzResetConnectorOffsetsVo {
                    ok: false,
                    failed_step: Some("stop".to_string()),
                    message,
                    connector_state: state,
                    steps,
                })),
            );
        }
        steps.push(DbzResetOffsetsStepVo {
            step: "stop".to_string(),
            ok: true,
            message: Some("连接器已停止".to_string()),
        });

        // 2. 删除位点
        if let Err(error) =
            KafkaConnectorUtils::reset_connector_offsets(&connect.url, &connector_name).await
        {
            let state = Self::current_state(&connect.url, &connector_name).await;
            steps.push(DbzResetOffsetsStepVo {
                step: "clear".to_string(),
                ok: false,
                message: Some(error.to_string()),
            });
            let message =
                format!("删除位点失败：{error}；连接器已停止但位点未清空，当前状态：{state}");
            return Ok(
                Ro::success(message.clone()).extra(Some(DbzResetConnectorOffsetsVo {
                    ok: false,
                    failed_step: Some("clear".to_string()),
                    message,
                    connector_state: state,
                    steps,
                })),
            );
        }
        steps.push(DbzResetOffsetsStepVo {
            step: "clear".to_string(),
            ok: true,
            message: Some("位点已清空".to_string()),
        });

        // 3. 可选恢复
        if dto.resume {
            if let Err(error) =
                KafkaConnectorUtils::resume_connector(&connect.url, &connector_name).await
            {
                let state = Self::current_state(&connect.url, &connector_name).await;
                steps.push(DbzResetOffsetsStepVo {
                    step: "resume".to_string(),
                    ok: false,
                    message: Some(error.to_string()),
                });
                let message = format!("恢复运行失败：{error}；位点已清空，连接器当前状态：{state}");
                return Ok(
                    Ro::success(message.clone()).extra(Some(DbzResetConnectorOffsetsVo {
                        ok: false,
                        failed_step: Some("resume".to_string()),
                        message,
                        connector_state: state,
                        steps,
                    })),
                );
            }
            steps.push(DbzResetOffsetsStepVo {
                step: "resume".to_string(),
                ok: true,
                message: Some("连接器已恢复运行".to_string()),
            });
        }

        // 回写状态：失败不影响操作结果，下一轮刷新会继续对齐
        if let Err(error) = Self::refresh().await {
            warn!("重置位点后刷新状态失败: {error}");
        }

        let state = Self::current_state(&connect.url, &connector_name).await;
        Ok(
            Ro::success("位点重置完成".to_string()).extra(Some(DbzResetConnectorOffsetsVo {
                ok: true,
                failed_step: None,
                message: if dto.resume {
                    "位点已清空并已恢复运行".to_string()
                } else {
                    "位点已清空，连接器保持停止".to_string()
                },
                connector_state: state,
                steps,
            })),
        )
    }

    /// # 读取连接器当前状态（用于失败提示）
    async fn current_state(base_url: &str, connector_name: &str) -> String {
        match KafkaConnectorUtils::fetch_connector_status(base_url, connector_name).await {
            Ok(health) => format!("{}", health.state),
            Err(KafkaConnectorError::NotFound) => "不存在".to_string(),
            Err(error) => format!("无法查询（{error}）"),
        }
    }

    async fn write_source(
        db: &DatabaseConnection,
        model: &SourceModel,
        status: ConnectorRunStatus,
        msg: Option<String>,
        now: i64,
    ) -> Result<(), SvcError> {
        if model.run_status == status.value() && model.run_msg == msg {
            return Ok(());
        }
        let mut active: SourceActiveModel = model.clone().into();
        active.run_status = Set(status.value());
        active.run_msg = Set(msg);
        active.updater_id = Set(0);
        active.update_ms = Set(now);
        DbzSourceConnectorDao::update(active, db)
            .await
            .map_err(|e| anyhow!("回写上游连接器<{}>状态失败: {e}", model.name))?;
        Ok(())
    }

    async fn write_sink(
        db: &DatabaseConnection,
        model: &SinkModel,
        status: ConnectorRunStatus,
        msg: Option<String>,
        now: i64,
    ) -> Result<(), SvcError> {
        if model.run_status == status.value() && model.run_msg == msg {
            return Ok(());
        }
        let mut active: SinkActiveModel = model.clone().into();
        active.run_status = Set(status.value());
        active.run_msg = Set(msg);
        active.updater_id = Set(0);
        active.update_ms = Set(now);
        DbzSinkConnectorDao::update(active, db)
            .await
            .map_err(|e| anyhow!("回写下游连接器<{}>状态失败: {e}", model.name))?;
        Ok(())
    }
}

/// # 刷新时需要的公共字段
trait ConnectorRow {
    fn run_status(&self) -> i16;
    fn run_msg(&self) -> Option<&str>;
    fn update_ms(&self) -> i64;
}

impl ConnectorRow for SourceModel {
    fn run_status(&self) -> i16 {
        self.run_status
    }

    fn run_msg(&self) -> Option<&str> {
        self.run_msg.as_deref()
    }

    fn update_ms(&self) -> i64 {
        self.update_ms
    }
}

impl ConnectorRow for SinkModel {
    fn run_status(&self) -> i16 {
        self.run_status
    }

    fn run_msg(&self) -> Option<&str> {
        self.run_msg.as_deref()
    }

    fn update_ms(&self) -> i64 {
        self.update_ms
    }
}
