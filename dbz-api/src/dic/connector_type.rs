use robotech::macros::dic;
use strum::{AsRefStr, Display, EnumString};

/// 只用于 Kafka Connect API，不落库、也不出现在任何 VO 里，
/// 没有语义上的默认值，所以**不声明 `default`**（即不生成 `Default` / `From<i16>`）。
#[dic]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr, Display, EnumString)]
#[strum(serialize_all = "kebab-case")]
pub enum ConnectorType {
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
