//! # Kafka broker 访问
//!
//! dbz 里唯一直接说 Kafka 协议的地方：探 broker 是否可用，以及往信号 topic 投递
//! `execute-snapshot`。除此之外它只跟 Kafka Connect 的 REST 接口打交道。
//!
//! ## 为什么用 `rskafka`
//!
//! `rdkafka` 更成熟，但要把 librdkafka 这层 C 库带进构建（cmake + libsasl），而这个
//! 工程已经在 `build-dependencies` 里用 vendored openssl 自己编了一套 C 依赖，再叠
//! 一层只会让构建更脆。`rskafka` 是纯 Rust（SASL 走 rsasl、TLS 走 rustls），代价是要
//! 自己管连接与超时 —— 也就是本模块做的事。注意它的默认特性会拉 lz4/zstd/snappy 的
//! C 绑定，`Cargo.toml` 里已关掉默认特性、只留 gzip。
//!
//! ## 支持的安全协议
//!
//! 只做 `PLAINTEXT` 与 `SASL_PLAINTEXT`（PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512）。
//! TLS 需要 CA 来源与证书校验策略，属于另一件事，暂时不在 `security_protocol` 里
//! 放出这两个取值 —— 与其让人选中一个用不了的值，不如先只给能用的。

use crate::dic::{KafkaSaslMechanism, KafkaSecurityProtocol};
use crate::mo::dbz_kafka_broker::Model as BrokerModel;
use anyhow::{anyhow, bail};
use rskafka::client::partition::UnknownTopicHandling;
use rskafka::client::{Client, ClientBuilder, Credentials, SaslConfig};
use rskafka::record::Record;
use std::time::{Duration, Instant};
use tokio::time::timeout;

/// 探活与投递的超时：broker 不可达时，TCP 握手会挂很久，不设上限会把接口拖死
const KAFKA_TIMEOUT: Duration = Duration::from_secs(10);

/// # 建立连接所需的一组值
///
/// `DbzKafkaBrokerVo` 故意不带密码（只进不出），而检测与投递信号都要用它，所以连接
/// 参数单独有个结构：由服务层从实体里取（实体带密码），之后一路传引用。放实体而不是
/// VO，是为了让「哪些代码碰过密码」在类型上就看得出来。
pub struct BrokerConn {
    pub name: String,
    pub bootstrap_servers: String,
    pub security_protocol: KafkaSecurityProtocol,
    pub sasl_mechanism: KafkaSaslMechanism,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
}

impl BrokerConn {
    /// # 从实体构造
    ///
    /// ## 参数
    /// * `model` - `dbz_kafka_broker` 的一行（含密码）
    pub fn from_model(model: &BrokerModel) -> Self {
        Self {
            name: model.name.clone(),
            bootstrap_servers: model.bootstrap_servers.clone(),
            security_protocol: model.security_protocol.into(),
            sasl_mechanism: model.sasl_mechanism.into(),
            sasl_username: model.sasl_username.clone(),
            sasl_password: model.sasl_password.clone(),
        }
    }
}

/// # broker 访问工具
pub struct KafkaBrokerUtils;

impl KafkaBrokerUtils {
    /// # 拆开 `host:port,host:port`
    ///
    /// 记录里存的是 Debezium 风格的 `bootstrap_servers`（逗号分隔），客户端要的是
    /// 逐个地址。空项直接丢掉：运维手填时常留尾逗号。
    pub fn split_servers(bootstrap_servers: &str) -> Vec<String> {
        bootstrap_servers
            .split(',')
            .map(str::trim)
            .filter(|server| !server.is_empty())
            .map(str::to_string)
            .collect()
    }

    /// # 校验并规整 `bootstrap_servers`
    ///
    /// 只做格式校验（`host:port`），不解析域名 —— 域名能不能解析是网络的事，写库时
    /// 判断不了。重复项会被去掉，避免同一个地址连两遍。
    ///
    /// ## 返回值
    /// * `Ok(String)` - 规整后的地址串（逗号分隔、无空格、无重复）
    /// * `Err(anyhow::Error)` - 空、缺端口、端口不是数字或越界
    pub fn normalize_servers(bootstrap_servers: &str) -> Result<String, anyhow::Error> {
        let servers = Self::split_servers(bootstrap_servers);
        if servers.is_empty() {
            bail!("bootstrap_servers 不能为空，格式为 host:port，多个用逗号分隔");
        }

        let mut normalized: Vec<String> = Vec::with_capacity(servers.len());
        for server in &servers {
            let (host, port) = server.rsplit_once(':').ok_or_else(|| {
                anyhow!("broker 地址<{server}>缺少端口，格式应为 host:port（如 10.0.0.1:9092）")
            })?;
            if host.is_empty() {
                bail!("broker 地址<{server}>缺少主机名");
            }
            let port: u16 = port
                .parse()
                .map_err(|_| anyhow!("broker 地址<{server}>的端口<{port}>不是合法端口"))?;
            if port == 0 {
                bail!("broker 地址<{server}>的端口不能为 0");
            }
            let one = format!("{host}:{port}");
            if !normalized.contains(&one) {
                normalized.push(one);
            }
        }
        Ok(normalized.join(","))
    }

    /// # 构造客户端
    ///
    /// 认证信息只在 `SASL_PLAINTEXT` 下装配；协议与机制不匹配时直接报错，而不是发出
    /// 一个注定被拒的连接 —— 那种错误在 broker 侧只体现为 `Authentication failed`，
    /// 排查时很难看出是配置写错了。
    fn build_builder(
        brokers: &[String],
        security_protocol: KafkaSecurityProtocol,
        sasl_mechanism: KafkaSaslMechanism,
        username: Option<&str>,
        password: Option<&str>,
    ) -> Result<ClientBuilder, anyhow::Error> {
        let mut builder = ClientBuilder::new(brokers.to_vec()).client_id("dbz-svr");

        match security_protocol {
            KafkaSecurityProtocol::Plaintext => {
                if sasl_mechanism != KafkaSaslMechanism::None {
                    bail!("安全协议为明文时不应配置 SASL 机制，请把机制改为 None");
                }
            }
            KafkaSecurityProtocol::SaslPlaintext => {
                if sasl_mechanism == KafkaSaslMechanism::None {
                    bail!(
                        "安全协议选择了 SASL 明文，但没有选择 SASL 机制（PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512）"
                    );
                }
                let username = username.unwrap_or_default();
                let password = password.unwrap_or_default();
                if username.is_empty() {
                    bail!("安全协议选择了 SASL 明文，但没有填认证用户名");
                }
                if password.is_empty() {
                    bail!("安全协议选择了 SASL 明文，但没有填认证密码");
                }
                let credentials = Credentials::new(username.to_string(), password.to_string());
                builder = builder.sasl_config(match sasl_mechanism {
                    KafkaSaslMechanism::Plain => SaslConfig::Plain(credentials),
                    KafkaSaslMechanism::ScramSha256 => SaslConfig::ScramSha256(credentials),
                    KafkaSaslMechanism::ScramSha512 => SaslConfig::ScramSha512(credentials),
                    KafkaSaslMechanism::None => unreachable!("上面已经拦掉"),
                });
            }
        }

        Ok(builder)
    }

    /// # 建立连接
    ///
    /// ## 返回值
    /// * `Ok(Client)` - 已完成 metadata 交换（`build()` 内部会拉一次）
    /// * `Err(anyhow::Error)` - 地址为空、配置矛盾、连不上、认证失败、超时
    pub async fn connect(conn: &BrokerConn) -> Result<Client, anyhow::Error> {
        let brokers = Self::split_servers(&conn.bootstrap_servers);
        if brokers.is_empty() {
            bail!("Kafka 集群<{}>没有配置 broker 地址", conn.name);
        }
        let builder = Self::build_builder(
            &brokers,
            conn.security_protocol,
            conn.sasl_mechanism,
            conn.sasl_username.as_deref(),
            conn.sasl_password.as_deref(),
        )?;

        // `build()` 里已经包含一次 metadata 请求，等于「连上并握过手」
        timeout(KAFKA_TIMEOUT, builder.build())
            .await
            .map_err(|_| {
                anyhow!(
                    "连接 Kafka 集群<{}>超时（{}s）：{}",
                    conn.name,
                    KAFKA_TIMEOUT.as_secs(),
                    brokers.join(",")
                )
            })?
            .map_err(|e| {
                anyhow!(
                    "连接 Kafka 集群<{}>失败（{}）：{e}",
                    conn.name,
                    brokers.join(",")
                )
            })
    }

    /// # 探活
    ///
    /// 拉一次 topic 列表：能拿到说明集群可读、认证通过、controller 正常。返回 topic
    /// 数量，调用方把它写进检测结果，让人有「确实连上了」的实感。
    ///
    /// ## 返回值
    /// * `Ok((u32, u64))` - topic 数量与耗时（毫秒）
    /// * `Err(anyhow::Error)` - 连接或查询失败
    pub async fn check(conn: &BrokerConn) -> Result<(u32, u64), anyhow::Error> {
        let started = Instant::now();
        let client = Self::connect(conn).await?;
        let topics = timeout(KAFKA_TIMEOUT, client.list_topics())
            .await
            .map_err(|_| anyhow!("查询 Kafka 集群<{}>的 topic 列表超时", conn.name))?
            .map_err(|e| anyhow!("查询 Kafka 集群<{}>的 topic 列表失败: {e}", conn.name))?;
        Ok((topics.len() as u32, started.elapsed().as_millis() as u64))
    }

    /// # 往信号 topic 投递一条信号
    ///
    /// Debezium 的 Kafka 信号通道有两个硬要求，写在参数说明里，改代码前先看：
    ///
    /// 1. 消息 **key 必须等于连接器的 `topic.prefix`**。对不上不会报错，只会被忽略。
    /// 2. value 是 `{"type": ..., "data": {...}}`，比信号表那套（`type` / `data` 分列）
    ///    多一层 `type`。
    ///
    /// 分区这里固定发 0 号：信号 topic 通常只有一个分区；多分区时 Debezium 的消费者
    /// 会订阅全部，落在哪个分区都能读到。
    ///
    /// ## 参数
    /// * `conn` - broker 连接参数（含密码）
    /// * `topic` - 信号 topic（连接器的 `signal.kafka.topic`）
    /// * `key` - 消息 key，必须等于连接器的 `topic.prefix`
    /// * `value` - 信号 JSON
    ///
    /// ## 返回值
    /// * `Ok(i64)` - 写入的 offset
    /// * `Err(anyhow::Error)` - 连接失败、topic 不存在、写入失败
    pub async fn send_signal(
        conn: &BrokerConn,
        topic: &str,
        key: &str,
        value: &str,
    ) -> Result<i64, anyhow::Error> {
        let client = Self::connect(conn).await?;
        let partition = timeout(
            KAFKA_TIMEOUT,
            client.partition_client(topic, 0, UnknownTopicHandling::Error),
        )
        .await
        .map_err(|_| {
            anyhow!(
                "等待 Kafka 集群<{}>返回 topic<{topic}>的元数据超时",
                conn.name
            )
        })?
        .map_err(|e| anyhow!("topic<{topic}>不可用（是否已创建？）：{e}"))?;

        let record = Record {
            key: Some(key.as_bytes().to_vec()),
            value: Some(value.as_bytes().to_vec()),
            headers: Default::default(),
            timestamp: chrono::Utc::now(),
        };

        let offsets = timeout(
            KAFKA_TIMEOUT,
            partition.produce(vec![record], Default::default()),
        )
        .await
        .map_err(|_| anyhow!("投递信号到 topic<{topic}>超时"))?
        .map_err(|e| anyhow!("投递信号到 topic<{topic}>失败: {e}"))?;

        offsets.first().copied().ok_or_else(|| {
            anyhow!("投递信号到 topic<{topic}>后没有返回 offset（多半是分区不可写）")
        })
    }
}
