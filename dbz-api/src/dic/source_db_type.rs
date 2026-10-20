use robotech::macros::dic;
use strum::{AsRefStr, Display, EnumString};

/// 上游连接器对应的**源库类型**：Debezium 能接的数据库方言。
///
/// 原名 `ConnectorType`，与 Kafka 的「连接器类型」（`source` / `sink`，见
/// [`KafkaConnectorVo`](crate::vo::KafkaConnectorVo) 的 `connector_type` 字段）撞名，
/// 但这里描述的是**源库方言**，与连接器方向无关，故改名。
///
/// 目前没有任何使用点（按类型生成 `connector.class` 的逻辑尚未实现），
/// 保留它是为了不改动既有模型，是否删除待定。
///
/// 只用于 Kafka Connect API，不落库、也不出现在任何 VO 里，
/// 没有语义上的默认值，所以**不声明 `default`**（即不生成 `Default` / `From<i16>`）。
#[dic]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr, Display, EnumString)]
#[strum(serialize_all = "kebab-case")]
pub enum SourceDbType {
    /// MySQL
    Mysql = 0,
    /// PostgreSQL
    Postgres = 1,
    /// MongoDB
    Mongodb = 2,
    /// SQL Server
    Sqlserver = 3,
    /// Oracle
    Oracle = 4,
    /// Db2
    Db2 = 5,
    /// Vitess
    Vitess = 6,
}
