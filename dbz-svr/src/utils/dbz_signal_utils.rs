//! # 源库信号工具（Debezium 信号表）
//!
//! dbz 里唯一一处**直连业务库**的代码。其它模块只跟 Kafka Connect 发 HTTP，而
//! 「再全量同步一次」这件事没有 REST 入口：Debezium 接收信号的方式就是往源库的
//! 信号表插一行，连接器的 source 通道从 binlog / WAL 里读到它才动作。
//!
//! 于是本模块只做三件事，边界划得很清楚：
//!
//! 1. **解析**连接器记录里的 `connector_config`：源库地址、账号、信号表名，以及
//!    捕获范围（`database.include.list` / `table.include.list` 等）。连接参数只认
//!    记录里那套，接口从不接受调用方直接传地址 —— 否则谁都能让 dbz 去连任意库。
//! 2. **算表**：用 `information_schema` 还原「连接器到底捕获了哪些表」，再按
//!    include / exclude 正则过滤。信号里的 `data-collections` 必须是全限定名
//!    （MySQL `库.表`、PG `模式.表`），而业务库动辄几百张表，让人手抄不现实。
//! 3. **发信号**：往信号表插一行 `execute-snapshot`。
//!
//! ## 为什么不用 Kafka Connect 的接口
//!
//! Kafka Connect 没有「重跑快照」的接口，能改的只有 `snapshot.mode`，而它只在
//! **没有 offset**（从没启动过，或位点已失效）时才起作用。删掉连接器再重建倒是能
//! 凑出一次全量，但要停服、要丢位点、还要下游清 topic，代价比插一行大得多。
//!
//! ## 前提（不满足时会给出可执行的提示）
//!
//! * 连接器配置里有 `signal.data.collection`（`source` 通道默认已启用）；
//! * 信号表已建好，且连接器账号有 INSERT 权限；
//! * 表有主键 —— 增量快照按主键分块，无主键表会被跳过；
//! * dbz 所在机器能直连源库。连接器能连不等于 dbz 能连，这条只能运行时才知道。

use crate::dto::SnapshotType;
use anyhow::{anyhow, bail};
use regex::Regex;
use serde_json::Value;
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions, MySqlSslMode};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};
use sqlx::{AssertSqlSafe, MySqlPool, PgPool, Row};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tracing::info;
use wheel_rs::time_utils::now_ms;

/// 表全限定名允许的字符
///
/// 信号表名会被直接拼进 INSERT 语句（占位符只能绑值、绑不了标识符），因此只放行
/// 简单标识符；带引号、点号、空格的表名直接拒绝，让人改用常规表名，而不是在这里
/// 做标识符转义。
static QUALIFIED_NAME_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[A-Za-z0-9_$\-#]+\.[A-Za-z0-9_$\-#]+$").unwrap());

/// 系统库 / 系统模式：即使 include 正则写成 `.*`，也不把它们算进快照范围
const SYSTEM_SCHEMAS: [&str; 7] = [
    "information_schema",
    "mysql",
    "performance_schema",
    "sys",
    "pg_catalog",
    "pg_toast",
    "pg_temp",
];

/// 一条信号最多带多少张表
///
/// `data` 列通常按官方 DDL 建成 `VARCHAR(2048)`，表名一多就放不下；与其拼出一条
/// 注定失败的超长 JSON，不如在发信号前拦住，让人分批触发。这个上限只是兜底，
/// 真正写不下时由数据库报错，见 [`describe_insert_error`]。
const MAX_DATA_COLLECTIONS: usize = 2000;

/// 连接源库的超时
///
/// 这个连接只在一次请求里用：连不上要尽快报错，不能把接口拖住。
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// 信号 id 的进程内序号
///
/// 信号表的 `id` 是主键，同一毫秒内的两次触发不能撞号；毫秒数加序号已经够唯一，
/// 也保住了可读性（出问题时能在日志里对上是哪一次触发）。
static SIGNAL_SEQ: AtomicU32 = AtomicU32::new(0);

/// # 源库方言
///
/// 只覆盖能用信号表的两种：MySQL / MariaDB 与 PostgreSQL。MongoDB 没有 SQL 信号表，
/// Oracle / SQL Server 的信号表是另一套 CDC 前置条件，都不在支持范围内。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceDialect {
    /// MySQL / MariaDB
    MySql,
    /// PostgreSQL
    PostgreSql,
}

impl SourceDialect {
    /// 对方言名，用于日志与接口返回
    pub fn value(&self) -> &'static str {
        match self {
            Self::MySql => "mysql",
            Self::PostgreSql => "postgresql",
        }
    }
}

/// # 从 `connector_config` 解析出的源库连接与信号信息
#[derive(Debug, Clone)]
pub struct SourceSignalConfig {
    /// 源库方言
    pub dialect: SourceDialect,
    /// 源库主机
    pub host: String,
    /// 源库端口
    pub port: u16,
    /// 连接器使用的账号
    pub user: String,
    /// 连接器使用的密码（可能为空，等同于无密码）
    pub password: String,
    /// 连接器连的库
    pub database: String,
    /// TLS 模式，取 `database.ssl.mode`（MySQL）/ `database.sslmode`（PG）
    pub ssl_mode: Option<String>,
    /// 信号表全限定名（`signal.data.collection`）
    pub signal_collection: String,
    /// 捕获的库 / 模式（可能是正则）
    pub schema_patterns: Vec<String>,
    /// `table.include.list`
    pub table_include: Vec<String>,
    /// `table.exclude.list`
    pub table_exclude: Vec<String>,
}

impl SourceSignalConfig {
    /// 只写表名时补全用的前缀
    ///
    /// MySQL 用连接器连的那个库；PG 用配置里的模式，多个模式（或写的是正则）时退回
    /// `public` —— 猜错比报错更糟，所以只在能确定时才用配置里的值。
    fn default_schema(&self) -> &str {
        match self.dialect {
            SourceDialect::MySql => &self.database,
            SourceDialect::PostgreSql => self
                .schema_patterns
                .first()
                .filter(|pattern| {
                    pattern
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '_')
                })
                .map_or("public", String::as_str),
        }
    }
}

/// # 捕获范围内可快照的表
#[derive(Debug, Clone, Default)]
pub struct CapturedTables {
    /// 全限定表名，按名称排序
    pub tables: Vec<String>,
    /// 解析过程中的提醒
    pub warnings: Vec<String>,
}

/// # 源库信号工具
pub struct DbzSignalUtils;

impl DbzSignalUtils {
    /// # 解析连接器配置
    ///
    /// 只读 `connector_config` 这一段 JSON，不连库。所有「这台连接器根本发不了信号」
    /// 的判断都收在这里，报错文案要能直接告诉人缺什么、加什么。
    ///
    /// ## 参数
    /// * `connector_config` - 连接器记录里的配置 JSON 文本
    ///
    /// ## 返回值
    /// * `Ok(SourceSignalConfig)` - 源库连接参数 + 信号表名 + 捕获范围
    /// * `Err(anyhow::Error)` - 配置不是 JSON、方言不支持、缺连接参数或信号配置
    pub fn parse_connector_config(
        connector_config: &str,
    ) -> Result<SourceSignalConfig, anyhow::Error> {
        let value: Value = serde_json::from_str(connector_config)
            .map_err(|e| anyhow!("连接器配置不是合法的 JSON: {e}"))?;
        let Value::Object(object) = value else {
            bail!("连接器配置必须是 JSON 对象");
        };
        let config: HashMap<String, Value> = object.into_iter().collect();

        let class = text(&config, "connector.class").unwrap_or_default();
        let class_lower = class.to_ascii_lowercase();
        let dialect = if class_lower.contains("mysql") || class_lower.contains("mariadb") {
            SourceDialect::MySql
        } else if class_lower.contains("postgres") {
            SourceDialect::PostgreSql
        } else {
            bail!(
                "连接器<{class}>不支持信号表触发快照：只有 MySQL/MariaDB 与 PostgreSQL 的 \
                 增量快照能用信号表，其它源库请按官方文档手工处理"
            );
        };

        // 信号表是硬前提：没有它，插不进信号，连接器也就永远收不到 execute-snapshot
        let Some(signal_collection) = text(&config, "signal.data.collection") else {
            bail!(
                "连接器配置里没有 signal.data.collection，发不了快照信号。请先在源库建信号表：\
                 CREATE TABLE debezium_signal (id VARCHAR(42) PRIMARY KEY, type VARCHAR(32) NOT NULL, \
                 data VARCHAR(2048) NULL)；再给连接器配置加上 \"signal.data.collection\": \
                 \"<库>.<信号表>\"（source 通道默认已启用，需要显式声明时加 \
                 \"signal.enabled.channels\": \"source\"），保存后由检查任务重新下发"
            );
        };
        let signal_collection = signal_collection.to_string();
        if !QUALIFIED_NAME_RE.is_match(&signal_collection) {
            bail!(
                "signal.data.collection<{signal_collection}>格式不对：要写成 `库.表`（MySQL）\
                 或 `模式.表`（PostgreSQL），且只含字母、数字、下划线、`$`、`#`、`-`"
            );
        }

        // source 通道是增量快照的必备通道；配了 kafka 却漏了 source 时，信号表插了也没人读
        let channels = list_config(&config, "signal.enabled.channels");
        if !channels.is_empty() && !channels.iter().any(|channel| channel == "source") {
            bail!(
                "连接器的 signal.enabled.channels 是 [{}]，不含 source：信号表里的信号读不到。\
                 请把 source 加进去（如 \"source,kafka\"）并重新下发配置",
                channels.join(",")
            );
        }

        let host = text(&config, "database.hostname")
            .ok_or_else(|| anyhow!("连接器配置里没有 database.hostname，定位不到源库"))?
            .to_string();
        let database = text(&config, "database.dbname")
            .ok_or_else(|| anyhow!("连接器配置里没有 database.dbname，不知道连哪个库"))?
            .to_string();
        let user = text(&config, "database.user")
            .ok_or_else(|| anyhow!("连接器配置里没有 database.user，没账号写信号表"))?
            .to_string();
        // 密码可以没有（有些库靠免密/证书认证），缺了就按空串连
        let password = text(&config, "database.password")
            .unwrap_or_default()
            .to_string();
        let default_port = match dialect {
            SourceDialect::MySql => 3306,
            SourceDialect::PostgreSql => 5432,
        };
        let port = text(&config, "database.port")
            .and_then(|port| port.parse::<u16>().ok())
            .unwrap_or(default_port);
        let ssl_mode = match dialect {
            SourceDialect::MySql => text(&config, "database.ssl.mode"),
            SourceDialect::PostgreSql => text(&config, "database.sslmode"),
        }
        .map(|mode| mode.to_string());

        // 捕获范围按 Debezium 的语义还原：include 列表 (MySQL 是库、PG 是模式) 缺省时
        // 就是连接器连的那个库；PG 的默认模式是 public
        let schema_patterns = match dialect {
            SourceDialect::MySql => {
                let databases = list_config(&config, "database.include.list");
                if databases.is_empty() {
                    vec![database.clone()]
                } else {
                    databases
                }
            }
            SourceDialect::PostgreSql => {
                let schemas = list_config(&config, "schema.include.list");
                if schemas.is_empty() {
                    vec!["public".to_string()]
                } else {
                    schemas
                }
            }
        };

        Ok(SourceSignalConfig {
            dialect,
            host,
            port,
            user,
            password,
            database,
            ssl_mode,
            signal_collection,
            schema_patterns,
            table_include: list_config(&config, "table.include.list"),
            table_exclude: list_config(&config, "table.exclude.list"),
        })
    }

    /// # 列出捕获范围内可快照的表
    ///
    /// 先连源库读一次 `information_schema.tables`，再按 `schema_patterns` /
    /// `table.include.list` / `table.exclude.list` 过滤，并永远排除信号表自己。
    ///
    /// ## 参数
    /// * `config` - [`parse_connector_config`](Self::parse_connector_config) 的结果
    ///
    /// ## 返回值
    /// * `Ok(CapturedTables)` - 表清单 + 提醒（可能为空清单，由调用方决定怎么报）
    /// * `Err(anyhow::Error)` - 连不上源库、查询失败或 include 正则写错
    pub async fn captured_tables(
        config: &SourceSignalConfig,
    ) -> Result<CapturedTables, anyhow::Error> {
        let pool = SourcePool::connect(config).await?;
        let rows = pool.fetch_base_tables().await;
        pool.close().await;
        Self::filter_captured(config, rows?)
    }

    /// # 补全并去重调用方给的表名
    ///
    /// 只写表名时补前缀（MySQL 补 `database.dbname`，PG 补模式），并挡掉拼不成
    /// 全限定名的写法 —— 与其让 Debezium 在日志里报一句难懂的错误，不如这里说清楚。
    ///
    /// ## 参数
    /// * `config` - 源库与捕获范围配置
    /// * `requested` - 调用方给的表名，可以带前缀也可以不带
    ///
    /// ## 返回值
    /// * `Ok(Vec<String>)` - 去重后的全限定名，保持调用方给的顺序
    /// * `Err(anyhow::Error)` - 有条目不是合法标识符
    pub fn normalize_data_collections(
        config: &SourceSignalConfig,
        requested: &[String],
    ) -> Result<Vec<String>, anyhow::Error> {
        normalize_names(config.default_schema(), requested)
    }

    /// # 拼信号里 `data` 的 JSON
    ///
    /// 形如 `{"data-collections":["库.表"],"type":"incremental"}`。
    ///
    /// ## 参数
    /// * `data_collections` - 已补全的全限定表名
    /// * `snapshot_type` - 增量或阻塞
    ///
    /// ## 返回值
    /// * `Ok(String)` - 写入信号表 `data` 列的 JSON
    /// * `Err(anyhow::Error)` - 表清单为空或超过上限
    pub fn build_signal_data(
        data_collections: &[String],
        snapshot_type: SnapshotType,
    ) -> Result<String, anyhow::Error> {
        if data_collections.is_empty() {
            bail!("表清单为空：`data-collections` 没有默认值，空清单会被 Debezium 当成无事可做");
        }
        if data_collections.len() > MAX_DATA_COLLECTIONS {
            bail!(
                "一条信号最多带 {MAX_DATA_COLLECTIONS} 张表，当前 {} 张：请分批触发",
                data_collections.len()
            );
        }
        let data = serde_json::json!({
            "data-collections": data_collections,
            "type": snapshot_type.value(),
        });
        Ok(data.to_string())
    }

    /// # 往信号表写一条 `execute-snapshot`
    ///
    /// 插入成功只代表**信号已发出**：Debezium 要等到自己读到这行变更才动手，
    /// 中间隔着一次 binlog 位点推进，所以接口不能承诺「快照已开始」。
    ///
    /// ## 参数
    /// * `config` - 源库与信号表配置
    /// * `data_collections` - 要快照的表（已补全）
    /// * `snapshot_type` - 增量或阻塞
    ///
    /// ## 返回值
    /// * `Ok(String)` - 信号 id（写进信号表的 `id` 列）
    /// * `Err(anyhow::Error)` - 连不上源库、SQL 失败（表不存在/无权限/data 列放不下）
    pub async fn send_snapshot_signal(
        config: &SourceSignalConfig,
        data_collections: &[String],
        snapshot_type: SnapshotType,
    ) -> Result<String, anyhow::Error> {
        let data = Self::build_signal_data(data_collections, snapshot_type)?;
        let signal_id = format!(
            "dbz-snap-{}-{}",
            now_ms(),
            SIGNAL_SEQ.fetch_add(1, Ordering::Relaxed) % 4096
        );

        let pool = SourcePool::connect(config).await?;
        let result = pool
            .insert_signal(
                &config.signal_collection,
                &signal_id,
                "execute-snapshot",
                &data,
            )
            .await;
        pool.close().await;
        result.map_err(|e| describe_insert_error(e, config))?;

        info!(
            "上游连接器<{}>的信号表<{}>已写入快照信号<{}>: type={}, 表 {} 张",
            config.database,
            config.signal_collection,
            signal_id,
            snapshot_type.value(),
            data_collections.len()
        );
        Ok(signal_id)
    }

    /// # 按捕获范围过滤 `information_schema` 查出来的表
    ///
    /// 过滤顺序与 Debezium 一致：先看库/模式是否在捕获范围，再看
    /// `table.include.list`，最后 `table.exclude.list` 一票否决。信号表自己永远排除
    /// —— 否则重快照会把信号表也扫一遍，而它的内容本来就是要被读走的。
    ///
    /// ## 参数
    /// * `config` - 源库与捕获范围配置
    /// * `rows` - `(库/模式, 表名)` 列表
    ///
    /// ## 返回值
    /// * `Ok(CapturedTables)` - 排序去重后的全限定名 + 没匹配到表的 include 条目
    /// * `Err(anyhow::Error)` - include / exclude 里有写错的正则
    fn filter_captured(
        config: &SourceSignalConfig,
        rows: Vec<(String, String)>,
    ) -> Result<CapturedTables, anyhow::Error> {
        let schemas = compile_patterns(&config.schema_patterns)?;
        let includes = compile_patterns(&config.table_include)?;
        let excludes = compile_patterns(&config.table_exclude)?;
        let mut include_hits = vec![false; includes.len()];
        let mut tables = Vec::new();

        for (schema, table) in rows {
            if SYSTEM_SCHEMAS
                .iter()
                .any(|system| schema.eq_ignore_ascii_case(system))
            {
                continue;
            }
            if !schemas.iter().any(|pattern| pattern.is_match(&schema)) {
                continue;
            }
            let qualified = format!("{schema}.{table}");
            if qualified == config.signal_collection {
                continue;
            }
            if !includes.is_empty() {
                let mut hit = false;
                for (index, pattern) in includes.iter().enumerate() {
                    if pattern.is_match(&qualified) {
                        include_hits[index] = true;
                        hit = true;
                    }
                }
                if !hit {
                    continue;
                }
            }
            if excludes.iter().any(|pattern| pattern.is_match(&qualified)) {
                continue;
            }
            tables.push(qualified);
        }
        tables.sort();
        tables.dedup();

        let warnings = config
            .table_include
            .iter()
            .zip(include_hits)
            .filter(|(_, hit)| !hit)
            .map(|(pattern, _)| {
                format!("table.include.list 里的<{pattern}>没有匹配到任何表，检查是不是写错了")
            })
            .collect();

        Ok(CapturedTables { tables, warnings })
    }
}

/// # 源库连接池
///
/// 两种方言的连接参数类型不同，但用法一样（查表、插信号、关掉），所以包一层枚举，
/// 把方言分支收敛在这一个类型里，上层不用到处 `match dialect`。
enum SourcePool {
    /// MySQL / MariaDB
    MySql(MySqlPool),
    /// PostgreSQL
    PostgreSql(PgPool),
}

impl SourcePool {
    /// # 连接源库
    ///
    /// 池子只开一条连接：一次请求只跑一两条语句，开池是为了复用连接、顺便借用
    /// sqlx 的超时与重试。用完即关，不常驻 —— dbz 不该长期持有业务库的连接。
    ///
    /// ## 参数
    /// * `config` - 源库连接参数
    ///
    /// ## 返回值
    /// * `Ok(SourcePool)` - 可用连接池
    /// * `Err(anyhow::Error)` - 连不上（网络不通、账号密码错、库不存在）
    async fn connect(config: &SourceSignalConfig) -> Result<Self, anyhow::Error> {
        let target = format!("{}:{}/{}", config.host, config.port, config.database);
        match config.dialect {
            SourceDialect::MySql => {
                let options = MySqlConnectOptions::new()
                    .host(&config.host)
                    .port(config.port)
                    .username(&config.user)
                    .password(&config.password)
                    .database(&config.database)
                    .ssl_mode(mysql_ssl_mode(config.ssl_mode.as_deref()));
                let pool = MySqlPoolOptions::new()
                    .max_connections(1)
                    .acquire_timeout(CONNECT_TIMEOUT)
                    .connect_with(options)
                    .await
                    .map_err(|e| anyhow!("连接源库<{target}>失败: {e}"))?;
                Ok(Self::MySql(pool))
            }
            SourceDialect::PostgreSql => {
                let options = PgConnectOptions::new()
                    .host(&config.host)
                    .port(config.port)
                    .username(&config.user)
                    .password(&config.password)
                    .database(&config.database)
                    .ssl_mode(pg_ssl_mode(config.ssl_mode.as_deref()));
                let pool = PgPoolOptions::new()
                    .max_connections(1)
                    .acquire_timeout(CONNECT_TIMEOUT)
                    .connect_with(options)
                    .await
                    .map_err(|e| anyhow!("连接源库<{target}>失败: {e}"))?;
                Ok(Self::PostgreSql(pool))
            }
        }
    }

    /// # 关闭连接池
    async fn close(self) {
        match self {
            Self::MySql(pool) => pool.close().await,
            Self::PostgreSql(pool) => pool.close().await,
        }
    }

    /// # 查询所有基础表
    ///
    /// 只查 `BASE TABLE`：视图、物化视图都不是 Debezium 的捕获对象。系统库不在这里
    /// 过滤（由 [`DbzSignalUtils::filter_captured`] 统一处理），因为 include 正则可能
    /// 就该匹配到某个系统库之外的库。
    ///
    /// ## 返回值
    /// * `Ok(Vec<(String, String)>)` - `(库/模式, 表名)`
    /// * `Err(anyhow::Error)` - 查询失败
    async fn fetch_base_tables(&self) -> Result<Vec<(String, String)>, anyhow::Error> {
        let sql = "select table_schema, table_name from information_schema.tables \
                   where table_type = 'BASE TABLE'";
        match self {
            Self::MySql(pool) => {
                let rows = sqlx::query(sql)
                    .fetch_all(pool)
                    .await
                    .map_err(|e| anyhow!("查询源库 information_schema 失败: {e}"))?;
                rows.iter()
                    .map(|row| {
                        Ok((
                            row.try_get::<String, _>("table_schema")?,
                            row.try_get::<String, _>("table_name")?,
                        ))
                    })
                    .collect::<Result<Vec<_>, sqlx::Error>>()
                    .map_err(|e| anyhow!("解析源库表清单失败: {e}"))
            }
            Self::PostgreSql(pool) => {
                let rows = sqlx::query(sql)
                    .fetch_all(pool)
                    .await
                    .map_err(|e| anyhow!("查询源库 information_schema 失败: {e}"))?;
                rows.iter()
                    .map(|row| {
                        Ok((
                            row.try_get::<String, _>("table_schema")?,
                            row.try_get::<String, _>("table_name")?,
                        ))
                    })
                    .collect::<Result<Vec<_>, sqlx::Error>>()
                    .map_err(|e| anyhow!("解析源库表清单失败: {e}"))
            }
        }
    }

    /// # 插入一条信号
    ///
    /// 两种方言只有占位符不同（`?` 与 `$1`），语句里的列名固定是 Debezium 要求的
    /// `id` / `type` / `data` 三列 —— 改了列名连接器就认不出来。
    ///
    /// 表名只能拼进语句（占位符绑不了标识符），因此用 `AssertSqlSafe` 显式声明
    /// 「这段 SQL 是安全的」：`collection` 在
    /// [`parse_connector_config`](DbzSignalUtils::parse_connector_config) 里已经过
    /// `库.表` 白名单校验，只剩字母数字与 `_$#-`，不含引号与空格。
    ///
    /// ## 参数
    /// * `collection` - 信号表全限定名（已校验过是简单标识符）
    /// * `id` - 信号 id
    /// * `signal_type` - 信号类型（`execute-snapshot`）
    /// * `data` - `data` 列的 JSON
    ///
    /// ## 返回值
    /// * `Ok(())` - 已写入
    /// * `Err(sqlx::Error)` - SQL 失败，由调用方翻译成可执行的提示
    async fn insert_signal(
        &self,
        collection: &str,
        id: &str,
        signal_type: &str,
        data: &str,
    ) -> Result<(), sqlx::Error> {
        match self {
            Self::MySql(pool) => {
                let sql = format!("insert into {collection} (id, type, data) values (?, ?, ?)");
                sqlx::query(AssertSqlSafe(sql))
                    .bind(id)
                    .bind(signal_type)
                    .bind(data)
                    .execute(pool)
                    .await
                    .map(|_| ())
            }
            Self::PostgreSql(pool) => {
                let sql = format!("insert into {collection} (id, type, data) values ($1, $2, $3)");
                sqlx::query(AssertSqlSafe(sql))
                    .bind(id)
                    .bind(signal_type)
                    .bind(data)
                    .execute(pool)
                    .await
                    .map(|_| ())
            }
        }
    }
}

/// # 把 include / exclude 条目编译成锚定的正则
///
/// Debezium 的 include 列表本身就是正则，语义是**整串匹配**，所以补上 `^` / `$`；
/// 写错了要在预览阶段就报出来，而不是等连接器日志。
fn compile_patterns(patterns: &[String]) -> Result<Vec<Regex>, anyhow::Error> {
    patterns
        .iter()
        .map(|pattern| {
            Regex::new(&format!("^(?:{pattern})$"))
                .map_err(|e| anyhow!("配置里的<{pattern}>不是合法正则: {e}"))
        })
        .collect()
}

/// # 补全并去重表名（两种信号通道共用）
///
/// 只写表名时补 `default_schema` 前缀，并挡掉拼不成全限定名的写法 —— 与其让
/// Debezium 在日志里报一句难懂的错误，不如这里说清楚。
///
/// ## 参数
/// * `default_schema` - 补全用的前缀（MySQL 是连接器连的库，PG 是模式）
/// * `requested` - 调用方给的表名，可以带前缀也可以不带
///
/// ## 返回值
/// * `Ok(Vec<String>)` - 去重后的全限定名，保持调用方给的顺序
/// * `Err(anyhow::Error)` - 有条目不是合法标识符
fn normalize_names(
    default_schema: &str,
    requested: &[String],
) -> Result<Vec<String>, anyhow::Error> {
    let mut seen = HashSet::new();
    let mut names = Vec::new();
    for item in requested {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        // 已经带了前缀就原样用；没默认前缀时（如 Kafka 通道缺 database.dbname）也只能原样用
        let qualified = if item.contains('.') || default_schema.is_empty() {
            item.to_string()
        } else {
            format!("{default_schema}.{item}")
        };
        if !QUALIFIED_NAME_RE.is_match(&qualified) {
            bail!(
                "表名<{item}>不合法：要写成 `库.表` 或 `模式.表`，且只含字母、数字、\
                 下划线、`$`、`#`、`-`"
            );
        }
        if seen.insert(qualified.clone()) {
            names.push(qualified);
        }
    }
    Ok(names)
}

/// # MySQL 的 TLS 模式
///
/// 未配置时按明文连：dbz 与源库通常同内网，而 `Preferred` 在客户端没编进 TLS 后端时
/// 会直接协商失败。`verify_ca` / `verify_identity` 需要证书文件，那在连接器所在机器
/// 上，dbz 拿不到，只能退到「必须加密」这一档。
fn mysql_ssl_mode(mode: Option<&str>) -> MySqlSslMode {
    match mode.map(str::to_ascii_lowercase).as_deref() {
        Some("preferred") => MySqlSslMode::Preferred,
        Some("required" | "verify_ca" | "verify_identity") => MySqlSslMode::Required,
        _ => MySqlSslMode::Disabled,
    }
}

/// # PostgreSQL 的 TLS 模式
///
/// 与 [`mysql_ssl_mode`] 同理：未配置时明文连，`verify-*` 退到「必须加密」。
fn pg_ssl_mode(mode: Option<&str>) -> PgSslMode {
    match mode.map(str::to_ascii_lowercase).as_deref() {
        Some("allow") => PgSslMode::Allow,
        Some("prefer" | "preferred") => PgSslMode::Prefer,
        Some("require" | "required" | "verify-ca" | "verify-full") => PgSslMode::Require,
        _ => PgSslMode::Disable,
    }
}

/// # 把写信号失败翻译成能照着做的提示
///
/// 这一步最容易踩的三个坑（表没建、没权限、`data` 列放不下）各有各的错法，
/// 直接把驱动原文抛出去，排查成本太高。
///
/// ## 参数
/// * `error` - 插入时的 SQL 错误
/// * `config` - 用于回显信号表名与账号
///
/// ## 返回值
/// * `anyhow::Error` - 带处理建议的错误
fn describe_insert_error(error: sqlx::Error, config: &SourceSignalConfig) -> anyhow::Error {
    let collection = &config.signal_collection;
    let Some(database_error) = error.as_database_error() else {
        return anyhow!("往信号表<{collection}>写信号失败: {error}");
    };
    let code = database_error.code().unwrap_or_default().to_string();
    let message = database_error.message();
    let hint = match (config.dialect, code.as_str()) {
        (SourceDialect::MySql, "1146") | (SourceDialect::PostgreSql, "42P01") => format!(
            "信号表<{collection}>不存在。dbz 是连源库 {}:{}/{} 去写信号的，表要建在那边\
             （不是 dbz 自己的库）：CREATE TABLE {collection} \
             (id VARCHAR(42) PRIMARY KEY, type VARCHAR(32) NOT NULL, data VARCHAR(2048) NULL)",
            config.host, config.port, config.database
        ),
        (SourceDialect::MySql, "1044" | "1142") | (SourceDialect::PostgreSql, "42501") => {
            format!(
                "账号<{}>没有信号表<{collection}>的 INSERT 权限",
                config.user
            )
        }
        _ if code == "22001" || message.to_ascii_lowercase().contains("too long") => {
            "表清单太长，信号表的 data 列放不下：把 data 列改成 TEXT，或分批触发快照".to_string()
        }
        _ => format!("源库返回: {message}（错误码 {code}）"),
    };
    anyhow!("往信号表<{collection}>写信号失败：{hint}")
}

/// # 读一个字符串配置项
///
/// 连接器配置里的值在 JSON 里可能是字符串，也可能是数字（`database.port` 这类常被
/// 写成裸值），统一按文本读；空串当成没配。
fn text<'a>(config: &'a HashMap<String, Value>, key: &str) -> Option<Cow<'a, str>> {
    match config.get(key)? {
        Value::String(value) => {
            let value = value.trim();
            (!value.is_empty()).then_some(Cow::Borrowed(value))
        }
        Value::Number(value) => Some(Cow::Owned(value.to_string())),
        _ => None,
    }
}

/// # 读一个逗号分隔的列表配置项
///
/// Debezium 的 `*.include.list` 都是逗号分隔，且允许条目里有正则的逗号吗？不允许 ——
/// 正则里要用逗号只能靠字符类，所以按逗号切是安全的。
fn list(value: Option<&str>) -> Vec<String> {
    value
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// # 读一个列表型配置项（逗号串或 JSON 数组都认）
///
/// Debezium 的列表类配置两种写法等价：`"orders,settle"` 与 `["orders","settle"]`。
/// 后者在 JSON 里更自然，从官方文档或别的连接器上抄配置时经常出现，而只认字符串
/// 会把「写了数组」当成「没配」—— 后果不是报错，而是捕获范围被悄悄放大成整库
/// （`database.include.list` 缺失时按 `database.dbname` 算），或者预检报「没有可快照的表」。
///
/// 数组的每个元素就是一个条目，不再按逗号二次切分：Debezium 就是这么解释的，
/// 元素里的逗号属于条目本身。
///
/// ## 参数
/// * `config` - 连接器配置
/// * `key` - 配置键
///
/// ## 返回值
/// * `Vec<String>` - 各条目，已去首尾空白并丢掉空项；未配置时为空
fn list_config(config: &HashMap<String, Value>, key: &str) -> Vec<String> {
    match config.get(key) {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|item| match item {
                Value::String(item) => {
                    let item = item.trim();
                    (!item.is_empty()).then(|| item.to_string())
                }
                // 端口一类纯数字项写成数字也收，省得为一个类型报「没配」
                Value::Number(item) => Some(item.to_string()),
                _ => None,
            })
            .collect(),
        _ => list(text(config, key).as_deref()),
    }
}

/// # 一个配置值是否等于「没填」
///
/// 只判值，不管键在不在。空串、纯空白、`null`、空数组（或元素全空）都算没填 ——
/// 前端把输入框清空时写的是 `""`，从别处抄来的配置偶尔是 `[]`。
fn is_blank_value(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::String(value) => value.trim().is_empty(),
        Value::Array(items) => items.iter().all(is_blank_value),
        _ => false,
    }
}

// ==========================================================================
// Kafka 信号通道
// ==========================================================================

/// # 从 `connector_config` 解析出的 Kafka 信号通道信息
///
/// Kafka 通道完全不碰源库：信号投到一个 topic，连接器的 `kafka` 通道自己消费。
/// 因此这里只需要三样东西 —— 信号 topic、消息 key（必须等于 `topic.prefix`）、
/// 以及捕获范围（用来拼 `data-collections` 正则）。
///
/// 与 [`SourceSignalConfig`] 的关键差别是**拿不到具体表名**：不连库就没法查
/// `information_schema`，只能把 `table.include.list` 那套正则原样交给 Debezium，
/// 由它在连接器侧展开。
#[derive(Debug, Clone)]
pub struct KafkaSignalConfig {
    /// 源库方言（决定 `库.表` 还是 `模式.表`）
    pub dialect: SourceDialect,
    /// 信号 topic（`signal.kafka.topic`）
    pub topic: String,
    /// 消息 key（`topic.prefix`，老版本叫 `database.server.name`）
    pub topic_prefix: String,
    /// 连接器连的那个库（`database.dbname`），只用来补全只写了表名的条目
    pub database: String,
    /// 连接器自己配的 `signal.kafka.bootstrap.servers`（消费侧地址）
    pub bootstrap_servers: Vec<String>,
    /// 捕获的库 / 模式
    pub schema_patterns: Vec<String>,
    /// `table.include.list`
    pub table_include: Vec<String>,
    /// `table.exclude.list`
    pub table_exclude: Vec<String>,
}

impl KafkaSignalConfig {
    /// 只写表名时补全用的前缀，取值规则与 [`SourceSignalConfig::default_schema`] 一致
    fn default_schema(&self) -> &str {
        match self.dialect {
            SourceDialect::MySql => &self.database,
            SourceDialect::PostgreSql => self
                .schema_patterns
                .first()
                .filter(|pattern| {
                    pattern
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '_')
                })
                .map_or("public", String::as_str),
        }
    }

    /// # 拼 `data-collections`
    ///
    /// 优先直接用 `table.include.list`：它本身就是「全限定名的正则」，与
    /// `data-collections` 要的语法一致。没有它就按捕获的库/模式退化成
    /// `(库1|库2)\..*`，再没有就 `.*`（连接器本来就没限范围）。
    ///
    /// ## 返回值
    /// * `Vec<String>` - 交给 Debezium 的正则列表，至少一项
    pub fn data_collection_patterns(&self) -> Vec<String> {
        if !self.table_include.is_empty() {
            return self.table_include.clone();
        }
        if !self.schema_patterns.is_empty() {
            return vec![format!("({})\\..*", self.schema_patterns.join("|"))];
        }
        vec![".*".to_string()]
    }
}

impl DbzSignalUtils {
    /// # 读出连接器启用了哪些信号通道
    ///
    /// 配置里没写 `signal.enabled.channels` 时按 Debezium 的默认值返回 `["source"]`
    /// —— 默认只启用 source，kafka 通道必须在配置里显式加上才会生效。
    ///
    /// ## 参数
    /// * `connector_config` - 连接器配置 JSON
    ///
    /// ## 返回值
    /// * `Vec<String>` - 通道名；配置不是合法 JSON 时为空
    pub fn enabled_channels(connector_config: &str) -> Vec<String> {
        let Ok(Value::Object(object)) = serde_json::from_str::<Value>(connector_config) else {
            return Vec::new();
        };
        let config: HashMap<String, Value> = object.into_iter().collect();
        let channels = list_config(&config, "signal.enabled.channels");
        if channels.is_empty() {
            vec!["source".to_string()]
        } else {
            channels
        }
    }

    /// # 补上 kafka 信号通道的 broker 地址
    ///
    /// `signal.kafka.bootstrap.servers` 在 Debezium 里是必填项、**没有默认值**，也不会
    /// 沿用 worker 自己的地址：留空时老版本在建 task 的路上直接
    /// `Properties.setProperty(key, null)` 抛 NPE，整个连接器起不来（3.1.2 起才改成
    /// 校验报错）。而 dbz 里开了 kafka 通道的配置，绝大多数就是「关联了哪个集群就发到
    /// 哪个」，所以由调用方把集群地址传进来，留空就填上，让人不用记这个键。
    ///
    /// 只填空值（缺失 / 空串 / 空数组）。用户自己填过的地址一律不动 —— 哪怕和集群
    /// 记录不一致，那也是他的选择，预检会提醒，不该在这里悄悄改掉。
    ///
    /// 序列化用 `to_string_pretty` 且键按字母序重排（`serde_json` 没开
    /// `preserve_order`）：只改这一处，值一个不丢。
    ///
    /// ## 参数
    /// * `connector_config` - 连接器配置 JSON
    /// * `bootstrap_servers` - 关联 Kafka 集群的地址（逗号分隔）
    ///
    /// ## 返回值
    /// * `Ok(None)` - 不用改（没开 kafka 通道，或已经有地址）
    /// * `Ok(Some(String))` - 补好地址的配置
    /// * `Err(anyhow::Error)` - 配置不是合法 JSON 对象，或集群地址为空
    pub fn fill_kafka_bootstrap(
        connector_config: &str,
        bootstrap_servers: &str,
    ) -> Result<Option<String>, anyhow::Error> {
        let bootstrap_servers = bootstrap_servers.trim();
        if !Self::enabled_channels(connector_config)
            .iter()
            .any(|channel| channel == "kafka")
        {
            return Ok(None);
        }
        if bootstrap_servers.is_empty() {
            bail!("关联的 Kafka 集群没有配置 broker 地址，没法补 signal.kafka.bootstrap.servers");
        }

        let mut value: Value = serde_json::from_str(connector_config)
            .map_err(|e| anyhow!("连接器配置不是合法的 JSON: {e}"))?;
        let Value::Object(object) = &mut value else {
            bail!("连接器配置必须是 JSON 对象");
        };
        const KEY: &str = "signal.kafka.bootstrap.servers";
        if object.get(KEY).is_some_and(|value| !is_blank_value(value)) {
            return Ok(None);
        }
        object.insert(
            KEY.to_string(),
            Value::String(bootstrap_servers.to_string()),
        );
        Ok(Some(serde_json::to_string_pretty(&value)?))
    }

    /// # 开了 kafka 通道但 broker 地址是空的
    ///
    /// 用来在下发前拦一道：这种配置在老版本 Debezium 上会直接抛 NPE，报错信息里
    /// 看不出跟这个键有关，先自己说清楚比等它崩强。
    ///
    /// 配置不是合法 JSON 时返回 `false` —— 那种情况由解析路径自己去报错。
    ///
    /// ## 参数
    /// * `connector_config` - 连接器配置 JSON
    ///
    /// ## 返回值
    /// * `bool` - 需要 kafka 通道且没填地址时为 `true`
    pub fn kafka_bootstrap_missing(connector_config: &str) -> bool {
        if !Self::enabled_channels(connector_config)
            .iter()
            .any(|channel| channel == "kafka")
        {
            return false;
        }
        let Ok(Value::Object(object)) = serde_json::from_str::<Value>(connector_config) else {
            return false;
        };
        let config: HashMap<String, Value> = object.into_iter().collect();
        list_config(&config, "signal.kafka.bootstrap.servers").is_empty()
    }

    /// # 解析 Kafka 信号通道配置
    ///
    /// 只校验「发一条 Kafka 信号需要什么」：通道得开着、topic 得有、`topic.prefix`
    /// 得有（消息 key 就是它，对不上会被连接器静默忽略）。
    ///
    /// ## 参数
    /// * `connector_config` - 连接器配置 JSON
    ///
    /// ## 返回值
    /// * `Ok(KafkaSignalConfig)` - topic、key 与捕获范围
    /// * `Err(anyhow::Error)` - 不是支持的方言、通道没开、缺 topic 或缺 key
    pub fn parse_kafka_config(connector_config: &str) -> Result<KafkaSignalConfig, anyhow::Error> {
        let value: Value = serde_json::from_str(connector_config)
            .map_err(|e| anyhow!("连接器配置不是合法的 JSON: {e}"))?;
        let Value::Object(object) = value else {
            bail!("连接器配置必须是 JSON 对象");
        };
        let config: HashMap<String, Value> = object.into_iter().collect();

        let class = text(&config, "connector.class").unwrap_or_default();
        let class_lower = class.to_ascii_lowercase();
        let dialect = if class_lower.contains("mysql") || class_lower.contains("mariadb") {
            SourceDialect::MySql
        } else if class_lower.contains("postgres") {
            SourceDialect::PostgreSql
        } else {
            bail!(
                "连接器<{class}>不支持用 Kafka 信号触发快照：只有 MySQL/MariaDB 与 PostgreSQL \
                 的增量快照接受 execute-snapshot 信号"
            );
        };

        let channels = Self::enabled_channels(connector_config);
        if !channels.iter().any(|channel| channel == "kafka") {
            bail!(
                "连接器的 signal.enabled.channels 是 [{}]，没有启用 kafka 通道。要么把它加上\
                 （如 \"source,kafka\"）并配好 signal.kafka.topic、\
                 signal.kafka.bootstrap.servers，要么改用 source 信号表",
                channels.join(",")
            );
        }

        let Some(topic) = text(&config, "signal.kafka.topic") else {
            bail!(
                "连接器启用了 kafka 信号通道，但没有配 signal.kafka.topic：信号要发到哪个 \
                 topic 上不知道"
            );
        };

        // key 必须是 topic.prefix；老版本 Debezium 叫 database.server.name
        let topic_prefix = text(&config, "topic.prefix")
            .or_else(|| text(&config, "database.server.name"))
            .ok_or_else(|| {
                anyhow!(
                    "连接器配置里没有 topic.prefix：Kafka 信号的消息 key 必须等于它，\
                     缺了就只能被连接器忽略"
                )
            })?;

        let database = text(&config, "database.dbname")
            .or_else(|| text(&config, "database.initial.database"))
            .unwrap_or_default()
            .to_string();
        let schema_patterns = match dialect {
            SourceDialect::MySql => {
                let databases = list_config(&config, "database.include.list");
                if databases.is_empty() {
                    if database.is_empty() {
                        Vec::new()
                    } else {
                        vec![database.clone()]
                    }
                } else {
                    databases
                }
            }
            SourceDialect::PostgreSql => {
                let schemas = list_config(&config, "schema.include.list");
                if schemas.is_empty() {
                    vec!["public".to_string()]
                } else {
                    schemas
                }
            }
        };

        Ok(KafkaSignalConfig {
            dialect,
            topic: topic.to_string(),
            topic_prefix: topic_prefix.to_string(),
            database,
            bootstrap_servers: list_config(&config, "signal.kafka.bootstrap.servers"),
            schema_patterns,
            table_include: list_config(&config, "table.include.list"),
            table_exclude: list_config(&config, "table.exclude.list"),
        })
    }

    /// # 补全并去重调用方给的表名（Kafka 通道）
    ///
    /// 与信号表那条的区别是**不连库**：只按连接器配置里的方言补前缀，不核对表是否
    /// 真的存在。Kafka 通道本来就把 `data-collections` 当正则交给 Debezium，写错的
    /// 表名不会报错、只会被跳过，所以这里的校验只到「拼得成全限定名」为止。
    ///
    /// ## 参数
    /// * `config` - Kafka 信号配置
    /// * `requested` - 调用方给的表名，可以带前缀也可以不带
    ///
    /// ## 返回值
    /// * `Ok(Vec<String>)` - 去重后的全限定名，保持调用方给的顺序
    /// * `Err(anyhow::Error)` - 有条目不是合法标识符
    pub fn normalize_kafka_data_collections(
        config: &KafkaSignalConfig,
        requested: &[String],
    ) -> Result<Vec<String>, anyhow::Error> {
        normalize_names(config.default_schema(), requested)
    }

    /// # 拼 Kafka 信号的消息体
    ///
    /// 注意与信号表的差别：表里 `type` 和 `data` 是两列，这里 `type` 要包在消息体里，
    /// `data` 是嵌套对象：
    ///
    /// ```text
    /// {"type":"execute-snapshot","data":{"data-collections":["db.t"],"type":"incremental"}}
    /// ```
    ///
    /// ## 参数
    /// * `data_collections` - 正则或全限定名列表
    /// * `snapshot_type` - 增量 / 阻塞
    ///
    /// ## 返回值
    /// * `Ok(String)` - 消息体 JSON
    /// * `Err(anyhow::Error)` - 清单为空
    pub fn build_kafka_signal_value(
        data_collections: &[String],
        snapshot_type: SnapshotType,
    ) -> Result<String, anyhow::Error> {
        let data = Self::build_signal_data(data_collections, snapshot_type)?;
        // data 已是合法 JSON，直接拼一层外层，省一次反序列化
        Ok(format!(r#"{{"type":"execute-snapshot","data":{data}}}"#))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 一份最小的 MySQL 连接器配置
    ///
    /// `extra` 是追加在末尾的配置片段（自带前导逗号），方便各用例只改动自己关心的项。
    fn mysql_config(extra: &str) -> String {
        format!(
            r#"{{
                "connector.class": "io.debezium.connector.mysql.MySqlConnector",
                "database.hostname": "10.0.0.1",
                "database.port": 3307,
                "database.user": "debezium",
                "database.password": "secret",
                "database.dbname": "orders",
                "database.include.list": "orders,settle",
                "table.include.list": "orders\\..*, settle\\.bill"
                {extra}
            }}"#
        )
    }

    /// 带信号表配置的那份，多数用例直接用这个
    fn mysql_config_with_signal(extra: &str) -> String {
        mysql_config(&format!(
            r#", "signal.data.collection": "orders.debezium_signal"{extra}"#
        ))
    }

    /// 把配置当纯文本解析：方言、端口、信号表、捕获范围都要对上
    #[test]
    fn parse_mysql_config() {
        let config = DbzSignalUtils::parse_connector_config(&mysql_config_with_signal("")).unwrap();
        assert_eq!(config.dialect, SourceDialect::MySql);
        assert_eq!(config.port, 3307);
        assert_eq!(config.database, "orders");
        assert_eq!(config.signal_collection, "orders.debezium_signal");
        assert_eq!(config.schema_patterns, vec!["orders", "settle"]);
        assert_eq!(config.table_include.len(), 2);
    }

    /// 缺 `signal.data.collection` 时要点明缺什么、怎么补
    #[test]
    fn reject_without_signal_collection() {
        let config = mysql_config("");
        let error = DbzSignalUtils::parse_connector_config(&config).unwrap_err();
        assert!(
            error.to_string().contains("signal.data.collection"),
            "{error}"
        );
    }

    /// `signal.enabled.channels` 里没有 source 时，信号表插了也没人读
    #[test]
    fn reject_channels_without_source() {
        let config = mysql_config_with_signal(r#", "signal.enabled.channels": "kafka""#);
        let error = DbzSignalUtils::parse_connector_config(&config).unwrap_err();
        assert!(error.to_string().contains("source"), "{error}");
    }

    /// 列表类配置写成 JSON 数组与逗号串等价
    ///
    /// 只认字符串时，数组会被当成「没配」：捕获范围从 `orders,settle` 悄悄退化成
    /// `database.dbname` 一个库，预检出来的表清单就不是用户以为的那些。
    #[test]
    fn accept_list_as_json_array() {
        let config = r#"{
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "database.hostname": "10.0.0.1",
            "database.user": "debezium",
            "database.dbname": "orders",
            "database.include.list": ["orders", "settle"],
            "table.include.list": ["orders\\..*", "settle\\.bill"],
            "table.exclude.list": ["orders\\.tmp_.*"],
            "signal.data.collection": "orders.debezium_signal",
            "signal.enabled.channels": ["source", "kafka"]
        }"#;
        let parsed = DbzSignalUtils::parse_connector_config(config).unwrap();
        assert_eq!(parsed.schema_patterns, vec!["orders", "settle"]);
        assert_eq!(parsed.table_include.len(), 2);
        assert_eq!(parsed.table_exclude.len(), 1);
        assert_eq!(
            DbzSignalUtils::enabled_channels(config),
            vec!["source", "kafka"]
        );
    }

    /// 数组里只写了 kafka 一样要被拦住 —— 通道判断走的是同一个解析
    #[test]
    fn reject_channels_without_source_in_array() {
        let config = r#"{
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "database.hostname": "10.0.0.1",
            "database.user": "debezium",
            "database.dbname": "orders",
            "signal.data.collection": "orders.debezium_signal",
            "signal.enabled.channels": ["kafka"]
        }"#;
        let error = DbzSignalUtils::parse_connector_config(config).unwrap_err();
        assert!(error.to_string().contains("source"), "{error}");
    }

    /// Kafka 通道：broker 地址与捕获范围写成数组也要认
    #[test]
    fn kafka_config_accepts_arrays() {
        let config = r#"{
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "topic.prefix": "orders",
            "database.dbname": "orders",
            "signal.kafka.topic": "dbz-signal",
            "signal.kafka.bootstrap.servers": ["10.0.0.21:9092", "10.0.0.22:9092"],
            "signal.enabled.channels": ["source", "kafka"],
            "table.include.list": ["orders\\..*"]
        }"#;
        let parsed = DbzSignalUtils::parse_kafka_config(config).unwrap();
        assert_eq!(parsed.bootstrap_servers.len(), 2);
        assert_eq!(parsed.table_include.len(), 1);
        assert_eq!(parsed.database, "orders");
    }

    /// 不是能用信号表的方言就直说
    #[test]
    fn reject_unsupported_dialect() {
        let config = mysql_config_with_signal("")
            .replace("mysql.MySqlConnector", "mongodb.MongoDbConnector");
        let error = DbzSignalUtils::parse_connector_config(&config).unwrap_err();
        assert!(error.to_string().contains("不支持"), "{error}");
    }

    /// 只写表名时按方言语义补前缀
    #[test]
    fn normalize_prefixes_table_name() {
        let config = DbzSignalUtils::parse_connector_config(&mysql_config_with_signal("")).unwrap();
        let names = DbzSignalUtils::normalize_data_collections(
            &config,
            &[
                "orders".to_string(),
                "settle.bill".to_string(),
                "orders".to_string(),
            ],
        )
        .unwrap();
        assert_eq!(names, vec!["orders.orders", "settle.bill"]);
    }

    /// 捕获范围按 include / exclude / 信号表三方过滤
    #[test]
    fn filter_captured_tables() {
        let config = DbzSignalUtils::parse_connector_config(&mysql_config_with_signal("")).unwrap();
        let rows = vec![
            ("orders".to_string(), "t_order".to_string()),
            ("orders".to_string(), "debezium_signal".to_string()),
            ("settle".to_string(), "bill".to_string()),
            ("settle".to_string(), "bill_item".to_string()),
            ("mysql".to_string(), "user".to_string()),
            ("other".to_string(), "x".to_string()),
        ];
        let captured = DbzSignalUtils::filter_captured(&config, rows).unwrap();
        assert_eq!(
            captured.tables,
            vec!["orders.t_order", "settle.bill"],
            "信号表、系统库、不在捕获范围的库都要排除"
        );
        assert!(captured.warnings.is_empty(), "{:?}", captured.warnings);
    }

    /// 信号 JSON 的键名固定，写错 Debezium 就不认
    #[test]
    fn build_signal_data_json() {
        let data = DbzSignalUtils::build_signal_data(
            &["orders.t_order".to_string()],
            SnapshotType::Incremental,
        )
        .unwrap();
        assert_eq!(
            data,
            r#"{"data-collections":["orders.t_order"],"type":"incremental"}"#
        );
    }

    /// 空清单会被当成无事可做，必须拦住
    #[test]
    fn reject_empty_signal_data() {
        assert!(DbzSignalUtils::build_signal_data(&[], SnapshotType::Blocking).is_err());
    }

    /// 未配置时按明文连；配了要求加密就升档
    ///
    /// `MySqlSslMode` / `PgSslMode` 没有 `PartialEq`，用 `Debug` 输出比较取值。
    #[test]
    fn ssl_mode_mapping() {
        assert_eq!(format!("{:?}", mysql_ssl_mode(None)), "Disabled");
        assert_eq!(
            format!("{:?}", mysql_ssl_mode(Some("required"))),
            "Required"
        );
        assert_eq!(format!("{:?}", pg_ssl_mode(None)), "Disable");
        assert_eq!(format!("{:?}", pg_ssl_mode(Some("verify-full"))), "Require");
    }

    /// 开了 kafka 通道又没填地址时补上，其它键一个都不能丢
    #[test]
    fn fill_kafka_bootstrap_when_missing() {
        let config = mysql_config_with_signal(r#", "signal.enabled.channels": "source,kafka""#);
        let filled = DbzSignalUtils::fill_kafka_bootstrap(&config, "10.0.0.21:9092,10.0.0.22:9092")
            .unwrap()
            .expect("开了 kafka 通道又没地址，应该补");
        let value: Value = serde_json::from_str(&filled).unwrap();
        assert_eq!(
            value["signal.kafka.bootstrap.servers"],
            "10.0.0.21:9092,10.0.0.22:9092"
        );
        assert_eq!(value["database.dbname"], "orders");
        assert_eq!(value["signal.enabled.channels"], "source,kafka");
    }

    /// 空串与空数组都算没填
    #[test]
    fn fill_kafka_bootstrap_when_blank() {
        for blank in ["\"\"", "[]"] {
            let config = mysql_config_with_signal(&format!(
                r#", "signal.enabled.channels": "kafka", "signal.kafka.bootstrap.servers": {blank}"#
            ));
            assert!(
                DbzSignalUtils::fill_kafka_bootstrap(&config, "10.0.0.21:9092")
                    .unwrap()
                    .is_some(),
                "地址写成 {blank} 也该补"
            );
        }
    }

    /// 用户自己填的地址不能改，哪怕和集群记录不一致
    #[test]
    fn fill_kafka_bootstrap_keeps_user_value() {
        let config = mysql_config_with_signal(
            r#", "signal.enabled.channels": "kafka", "signal.kafka.bootstrap.servers": "10.9.9.9:9092""#,
        );
        assert!(
            DbzSignalUtils::fill_kafka_bootstrap(&config, "10.0.0.21:9092")
                .unwrap()
                .is_none()
        );
    }

    /// 没开 kafka 通道时不该往配置里塞这个键
    #[test]
    fn fill_kafka_bootstrap_skips_source_only() {
        let config = mysql_config_with_signal(r#", "signal.enabled.channels": "source""#);
        assert!(
            DbzSignalUtils::fill_kafka_bootstrap(&config, "10.0.0.21:9092")
                .unwrap()
                .is_none()
        );
    }

    /// 下发前的拦截与补齐用同一份判断
    #[test]
    fn detect_missing_kafka_bootstrap() {
        let missing = mysql_config_with_signal(r#", "signal.enabled.channels": "source,kafka""#);
        assert!(DbzSignalUtils::kafka_bootstrap_missing(&missing));
        let filled = mysql_config_with_signal(
            r#", "signal.enabled.channels": "source,kafka", "signal.kafka.bootstrap.servers": "10.0.0.21:9092""#,
        );
        assert!(!DbzSignalUtils::kafka_bootstrap_missing(&filled));
        let source_only = mysql_config_with_signal(r#", "signal.enabled.channels": "source""#);
        assert!(!DbzSignalUtils::kafka_bootstrap_missing(&source_only));
    }
}
