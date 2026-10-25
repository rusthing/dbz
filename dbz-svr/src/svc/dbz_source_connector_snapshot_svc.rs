//! # 上游连接器「再同步」服务
//!
//! 把「再全量同步一次」包成两步：预检（只读，列出会快照哪些表）与触发（发一条
//! `execute-snapshot` 信号）。信号怎么送到 Debezium 有两条路，见
//! [`SignalChannel`](crate::dto::SignalChannel)：
//!
//! * `source` —— 往源库的信号表插一行，需要 dbz 直连业务库；
//! * `kafka` —— 往信号 topic 投一条消息，不碰业务库，但要有 broker 记录。
//!
//! 两者在连接器侧是等价的（都是「连接器自己读到信号再动手」），所以本服务只负责
//! **挑一条可用的路**并把信号送出去，不关心连接器怎么消费。
//!
//! 与实体服务不同，本服务**不写本库**：读一条 `dbz_source_connector` 记录取配置，
//! 改的是源库的信号表或 Kafka 的 topic。因此不走 `#[svc]` 宏，手写 `impl` 块。

use crate::dto::{
    DbzSourceConnectorSnapshotDto, DbzSourceConnectorSnapshotPreviewDto, SignalChannel,
};
use crate::svc::{DbzKafkaBrokerSvc, DbzKafkaConnectSvc, DbzSourceConnectorSvc};
use crate::utils::{
    BrokerConn, DbzSignalUtils, KafkaBrokerUtils, KafkaSignalConfig, SourceSignalConfig,
};
use crate::vo::{DbzSourceConnectorSnapshotVo, DbzSourceConnectorVo};
use anyhow::{anyhow, bail};
use robotech::api::{Ro, U64};
use robotech::macros::log_call;
use sea_orm::DatabaseConnection;
use tracing::info;

/// 没指定通道时的尝试顺序
///
/// `source` 在前：它是 Debezium 默认启用的通道，不依赖 broker 记录，对既有连接器
/// 而言是「本来就能用」的那条；`kafka` 排后意味着只有 source 不可用时才会自动切过
/// 去，切换会在预检结果里留一条提醒。
const DEFAULT_CHANNEL_ORDER: [SignalChannel; 2] = [SignalChannel::Source, SignalChannel::Kafka];

/// # 一次再同步实际走的通道
enum SignalTarget {
    /// 源库信号表
    Source(SourceSignalConfig),
    /// Kafka 信号 topic
    Kafka {
        /// 信号 topic、消息 key 与捕获范围
        config: KafkaSignalConfig,
        /// 投递消息用的集群（含认证）
        conn: BrokerConn,
        /// 集群名，用在提示文案里
        broker_name: String,
    },
}

/// # 通道解析结果
struct Resolved {
    /// 最终选中的通道
    target: SignalTarget,
    /// 这台连接器上可用的通道（给前端画选项）
    channels: Vec<String>,
    /// 解析过程中的提醒（如从 source 退到 kafka 的原因）
    warnings: Vec<String>,
}

/// # 上游连接器再同步服务
pub struct DbzSourceConnectorSnapshotSvc;

impl DbzSourceConnectorSnapshotSvc {
    /// # 预检：这次会快照哪些表
    ///
    /// `source` 通道会解析配置 + 读一次源库的 `information_schema`；`kafka` 通道不
    /// 连库，直接把 `table.include.list` 当成表范围返回。两种都只读，不写任何东西。
    /// 前端拿它填「选择表」的下拉，也用它把人挡在「信号通道配了吗」「dbz 连得上源库
    /// 吗」这些问题之前 —— 真发信号时才发现不通，体验太差。
    ///
    /// ## 参数
    /// * `dto` - 上游连接器ID + 想看的通道（可空）
    ///
    /// ## 返回值
    /// * `Ok(Ro<DbzSourceConnectorSnapshotVo>)` - 通道、信号落点、表清单与提醒
    /// * `Err(anyhow::Error)` - 记录不存在/已禁用、配置不支持、连不上源库
    #[log_call]
    pub async fn preview(
        dto: DbzSourceConnectorSnapshotPreviewDto,
    ) -> Result<Ro<DbzSourceConnectorSnapshotVo>, anyhow::Error> {
        let source = Self::load(dto.id).await?;
        let Resolved {
            target,
            channels,
            mut warnings,
        } = Self::resolve(&source, dto.channel).await?;

        let vo = match target {
            SignalTarget::Source(config) => {
                let captured = DbzSignalUtils::captured_tables(&config).await?;
                if captured.tables.is_empty() {
                    bail!(
                        "连接器<{}>的捕获范围里没有可快照的表：检查 database.include.list / \
                         table.include.list 是否写得太窄",
                        source.name
                    );
                }
                warnings.extend(captured.warnings);
                DbzSourceConnectorSnapshotVo {
                    dialect: config.dialect.value().to_string(),
                    database: config.database.clone(),
                    channels,
                    channel: SignalChannel::Source.value().to_string(),
                    signal_collection: Some(config.signal_collection),
                    kafka_topic: None,
                    kafka_broker: None,
                    data_collections: captured.tables,
                    warnings,
                }
            }
            SignalTarget::Kafka {
                config,
                conn,
                broker_name,
            } => {
                warnings.extend(Self::broker_mismatch_warning(&config, &conn, &broker_name));
                let patterns = config.data_collection_patterns();
                if patterns.iter().all(|pattern| pattern == ".*") {
                    warnings.push(
                        "连接器既没有 table.include.list 也没有可用的库/模式范围：信号里的 \
                         data-collections 是 .*，会快照捕获到的所有表"
                            .to_string(),
                    );
                }
                DbzSourceConnectorSnapshotVo {
                    dialect: config.dialect.value().to_string(),
                    database: config.database.clone(),
                    channels,
                    channel: SignalChannel::Kafka.value().to_string(),
                    signal_collection: None,
                    kafka_topic: Some(config.topic.clone()),
                    kafka_broker: Some(broker_name),
                    data_collections: patterns,
                    warnings,
                }
            }
        };

        let summary = match &vo.channel {
            channel if channel == SignalChannel::Kafka.value() => format!(
                "Kafka 通道：{} 项表范围，信号投到 topic<{}>",
                vo.data_collections.len(),
                vo.kafka_topic.as_deref().unwrap_or_default()
            ),
            _ => format!("信号表通道：{} 张可快照的表", vo.data_collections.len()),
        };
        Ok(Ro::success(summary).extra(Some(vo)))
    }

    /// # 触发快照（发信号）
    ///
    /// 调用方没指定表时用连接器捕获范围内的全部表；指定了就逐个核对 —— `source`
    /// 通道下会核对是不是真的在捕获范围内（Debezium 对范围外的表只会打一行日志然后
    /// 跳过，不如这里直接拒绝），`kafka` 通道不连库，只核对格式。
    ///
    /// 返回成功**只代表信号已送出**：连接器要等自己读到这条信号才动手，中间隔着一次
    /// 位点推进或一次轮询，所以这里不说「快照已开始」。
    ///
    /// ## 参数
    /// * `dto` - 上游连接器ID + 表清单（可空）+ 快照类型 + 通道（可空）
    ///
    /// ## 返回值
    /// * `Ok(Ro<()>)` - 信号已送出，`msg` 里带落点与表数量
    /// * `Err(anyhow::Error)` - 表不在捕获范围、连不上源库/broker、发信号失败
    #[log_call]
    pub async fn snapshot(dto: DbzSourceConnectorSnapshotDto) -> Result<Ro<()>, anyhow::Error> {
        let source = Self::load(dto.id).await?;
        let Resolved { target, .. } = Self::resolve(&source, dto.channel).await?;
        let snapshot_type = dto.snapshot_type;

        match target {
            SignalTarget::Source(config) => {
                let captured = DbzSignalUtils::captured_tables(&config).await?;
                let tables = if dto.data_collections.is_empty() {
                    captured.tables
                } else {
                    let requested =
                        DbzSignalUtils::normalize_data_collections(&config, &dto.data_collections)?;
                    let unknown: Vec<&str> = requested
                        .iter()
                        .filter(|table| !captured.tables.contains(table))
                        .map(String::as_str)
                        .collect();
                    if !unknown.is_empty() {
                        bail!(
                            "以下表不在连接器<{}>的捕获范围内：{}。可先用预检接口看看捕获范围",
                            source.name,
                            unknown.join("、")
                        );
                    }
                    requested
                };
                if tables.is_empty() {
                    bail!(
                        "连接器<{}>的捕获范围里没有可快照的表：检查 database.include.list / \
                         table.include.list 是否写得太窄",
                        source.name
                    );
                }

                let signal_id =
                    DbzSignalUtils::send_snapshot_signal(&config, &tables, snapshot_type).await?;

                info!(
                    "上游连接器<{}>(id={}): 已写入 {} 快照信号<{}>，共 {} 张表",
                    source.name,
                    source.id.value(),
                    snapshot_type.value(),
                    signal_id,
                    tables.len()
                );
                Ok(Ro::success(format!(
                    "已写入 {} 快照信号<{}>（{} 张表，信号表 {}）：Debezium 读到后开始快照，\
                     进度可在「远端连接器」里看任务状态；快照数据以 op=r 追加到原 topic，\
                     下游需按主键幂等处理",
                    snapshot_type.value(),
                    signal_id,
                    tables.len(),
                    config.signal_collection
                )))
            }
            SignalTarget::Kafka {
                config,
                conn,
                broker_name,
            } => {
                let tables = if dto.data_collections.is_empty() {
                    config.data_collection_patterns()
                } else {
                    DbzSignalUtils::normalize_kafka_data_collections(
                        &config,
                        &dto.data_collections,
                    )?
                };
                let value = DbzSignalUtils::build_kafka_signal_value(&tables, snapshot_type)?;
                let offset = KafkaBrokerUtils::send_signal(
                    &conn,
                    &config.topic,
                    &config.topic_prefix,
                    &value,
                )
                .await?;

                info!(
                    "上游连接器<{}>(id={}): 已向集群<{}>的 topic<{}> 投递 {} 快照信号，\
                     key={}, offset={offset}, {} 项表范围",
                    source.name,
                    source.id.value(),
                    broker_name,
                    config.topic,
                    snapshot_type.value(),
                    config.topic_prefix,
                    tables.len()
                );
                Ok(Ro::success(format!(
                    "已向 Kafka 集群<{}>的 topic<{}> 投递 {} 快照信号（offset={offset}，\
                     {} 项表范围）：连接器读到后开始快照，进度可在「远端连接器」里看任务状态；\
                     快照数据以 op=r 追加到原 topic，下游需按主键幂等处理",
                    broker_name,
                    config.topic,
                    snapshot_type.value(),
                    tables.len()
                )))
            }
        }
    }

    /// # 读记录并确认可用
    ///
    /// 两个接口的共同前置：记录得在、连接器得是启用状态。禁用时直接拒绝 —— 信号
    /// 会留在信号表/topic 里，等连接器启用后才被读走，容易让人误以为「点了没反应」。
    ///
    /// ## 参数
    /// * `id` - 上游连接器ID
    ///
    /// ## 返回值
    /// * `Ok(DbzSourceConnectorVo)` - 连接器记录
    /// * `Err(anyhow::Error)` - 不存在或已禁用
    async fn load(id: U64) -> Result<DbzSourceConnectorVo, anyhow::Error> {
        let source = DbzSourceConnectorSvc::get_by_id::<DatabaseConnection>(id, None)
            .await
            .map_err(|e| anyhow!("查询上游连接器<{}>失败: {e}", id.value()))?
            .extra
            .ok_or_else(|| anyhow!("上游连接器<{}>不存在", id.value()))?;

        if !source.enabled {
            bail!(
                "上游连接器<{}>已禁用：信号会积压，等连接器启用后才被读取，请先启用再触发",
                source.name
            );
        }

        Ok(source)
    }

    /// # 这台连接器上可用的信号通道
    ///
    /// 按 [`DEFAULT_CHANNEL_ORDER`] 排序，只认已知的两条通道（配置里写了别的值不算可用）。
    /// 结果与「本次走哪条」无关：同一个连接器两次预检必须给出同一份清单，否则前端
    /// 的切换项会随选择一起缩水。
    ///
    /// ## 参数
    /// * `channels` - `signal.enabled.channels` 解析出来的取值
    ///
    /// ## 返回值
    /// * `Vec<String>` - 可用通道名，最多两项
    fn available_channels(channels: &[String]) -> Vec<String> {
        DEFAULT_CHANNEL_ORDER
            .into_iter()
            .filter(|channel| channels.iter().any(|item| item == channel.value()))
            .map(|channel| channel.value().to_string())
            .collect()
    }

    /// # 选一条能用的信号通道
    ///
    /// 指定了通道就只用那一条，失败直接报错（人已经明确选了，悄悄换路更糟）；没指定
    /// 就按 [`DEFAULT_CHANNEL_ORDER`] 依次尝试，把前面失败的原因记进提醒。
    ///
    /// ## 参数
    /// * `source` - 上游连接器记录
    /// * `channel` - 调用方指定的通道，可空
    ///
    /// ## 返回值
    /// * `Ok(Resolved)` - 通道、可用通道列表与提醒
    /// * `Err(anyhow::Error)` - 没有可用通道，或指定通道不可用
    async fn resolve(
        source: &DbzSourceConnectorVo,
        channel: Option<SignalChannel>,
    ) -> Result<Resolved, anyhow::Error> {
        let channels = DbzSignalUtils::enabled_channels(&source.connector_config);
        let enabled = |channel: SignalChannel| channels.iter().any(|item| item == channel.value());

        // 可用的通道与「本次走哪条」是两件事：前端拿它画切换项，一旦跟着入参收窄成
        // 一条，用户切过去就再也切不回来（选项直接消失）。
        let available = Self::available_channels(&channels);

        let order: Vec<SignalChannel> = match channel {
            Some(channel) => {
                if !enabled(channel) {
                    bail!(
                        "连接器<{}>没有启用 {} 信号通道（signal.enabled.channels = [{}]）：\
                         source 通道要在源库建信号表并配 signal.data.collection，\
                         kafka 通道要配上 signal.kafka.topic 与 signal.kafka.bootstrap.servers",
                        source.name,
                        channel.value(),
                        channels.join(",")
                    );
                }
                vec![channel]
            }
            None => DEFAULT_CHANNEL_ORDER
                .into_iter()
                .filter(|channel| enabled(*channel))
                .collect(),
        };
        if order.is_empty() {
            bail!(
                "连接器<{}>没有启用任何信号通道（signal.enabled.channels 为空）：\
                 再同步只能靠信号触发，请先启用 source 或 kafka 通道",
                source.name
            );
        }

        let explicit = channel.is_some();
        let mut errors: Vec<String> = Vec::new();
        for channel in order {
            match Self::build_target(source, channel).await {
                Ok(target) => {
                    let warnings = errors
                        .into_iter()
                        .map(|error| format!("已跳过不可用的通道：{error}"))
                        .collect();
                    return Ok(Resolved {
                        target,
                        channels: available,
                        warnings,
                    });
                }
                Err(e) => {
                    if explicit {
                        return Err(e);
                    }
                    errors.push(format!("{} 通道不可用：{e:#}", channel.value()));
                }
            }
        }

        bail!(
            "连接器<{}>没有可用的信号通道：{}",
            source.name,
            errors.join("；")
        )
    }

    /// # 按通道准备发信号需要的材料
    async fn build_target(
        source: &DbzSourceConnectorVo,
        channel: SignalChannel,
    ) -> Result<SignalTarget, anyhow::Error> {
        match channel {
            SignalChannel::Source => Ok(SignalTarget::Source(
                DbzSignalUtils::parse_connector_config(&source.connector_config)?,
            )),
            SignalChannel::Kafka => {
                let config = DbzSignalUtils::parse_kafka_config(&source.connector_config)?;
                let (broker_name, conn) = Self::load_broker(source).await?;
                Ok(SignalTarget::Kafka {
                    config,
                    conn,
                    broker_name,
                })
            }
        }
    }

    /// # 找连接器所在 Kafka Connect 关联的集群
    ///
    /// 信号 topic 在哪个集群上，连接器就部署在哪台 worker 上，所以顺着
    /// `kafka_connect_id → kafka_broker_id` 找过去。老记录可能没挂集群，这里给出
    /// 可执行的提示而不是「为空」。
    ///
    /// ## 返回值
    /// * `Ok((String, BrokerConn))` - 集群名 + 连接参数（含认证）
    /// * `Err(anyhow::Error)` - Connect 不存在、没挂集群、集群记录不存在
    async fn load_broker(
        source: &DbzSourceConnectorVo,
    ) -> Result<(String, BrokerConn), anyhow::Error> {
        let kafka_connect =
            DbzKafkaConnectSvc::get_by_id::<DatabaseConnection>(source.kafka_connect_id, None)
                .await
                .map_err(|e| {
                    anyhow!(
                        "查询 Kafka Connect<{}>失败: {e}",
                        source.kafka_connect_id.value()
                    )
                })?
                .extra
                .ok_or_else(|| {
                    anyhow!("Kafka Connect<{}>不存在", source.kafka_connect_id.value())
                })?;

        let broker_id = kafka_connect.kafka_broker_id.ok_or_else(|| {
            anyhow!(
                "Kafka Connect<{}>没有关联 Kafka 集群：Kafka 信号要投到某个集群上，\
                 请先给这台 Connect 选一个集群",
                kafka_connect.name
            )
        })?;

        let conn = DbzKafkaBrokerSvc::load_conn(broker_id).await?;
        let name = conn.name.clone();
        Ok((name, conn))
    }

    /// # 连接器消费信号用的地址与我们投递的集群是否一致
    ///
    /// `signal.kafka.bootstrap.servers` 没配时连接器沿用 worker 自己的地址，多半就是
    /// 同一个集群，不用提醒；配了却和集群记录不一样，说明两边看的不是一套 Kafka，
    /// 信号投出去也没人读 —— 这种错不报出来只能靠翻连接器日志。
    ///
    /// ## 返回值
    /// * `Vec<String>` - 不一致时的提醒，一致或没配时为空
    fn broker_mismatch_warning(
        config: &KafkaSignalConfig,
        conn: &BrokerConn,
        broker_name: &str,
    ) -> Vec<String> {
        if config.bootstrap_servers.is_empty() {
            return Vec::new();
        }
        let mut ours = KafkaBrokerUtils::split_servers(&conn.bootstrap_servers);
        let mut theirs = config.bootstrap_servers.clone();
        ours.sort();
        theirs.sort();
        if ours == theirs {
            return Vec::new();
        }
        vec![format!(
            "连接器的 signal.kafka.bootstrap.servers 是 [{}]，与集群<{}>的地址 [{}] 不同：\
             信号会投到集群<{}>，连接器却从自己的地址消费，两边不是同一套 Kafka 时读不到信号",
            theirs.join(","),
            broker_name,
            ours.join(","),
            broker_name
        )]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 两条都开着时按默认顺序给出，前端据此画两个切换项
    #[test]
    fn available_channels_keeps_default_order() {
        let channels = vec!["kafka".to_string(), "source".to_string()];
        assert_eq!(
            DbzSourceConnectorSnapshotSvc::available_channels(&channels),
            ["source".to_string(), "kafka".to_string()]
        );
    }

    /// 只开了一条时只给一条 —— 单选项没必要画
    #[test]
    fn available_channels_single() {
        assert_eq!(
            DbzSourceConnectorSnapshotSvc::available_channels(&["kafka".to_string()]),
            vec!["kafka".to_string()]
        );
    }

    /// 配置里写了别的通道名不算可用
    #[test]
    fn available_channels_ignores_unknown() {
        let channels = vec!["source".to_string(), "outbox".to_string()];
        assert_eq!(
            DbzSourceConnectorSnapshotSvc::available_channels(&channels),
            vec!["source".to_string()]
        );
    }
}
