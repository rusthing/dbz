use robotech::macros::dic;
use strum::{AsRefStr, Display, EnumString};

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

impl Default for ConnectorType {
    fn default() -> Self {
        Self::Mysql
    }
}

impl From<ConnectorType> for i16 {
    fn from(val: ConnectorType) -> Self {
        val as i16
    }
}

impl From<i16> for ConnectorType {
    fn from(value: i16) -> Self {
        Self::from_i16(value).unwrap_or_default()
    }
}

impl ConnectorType {
    pub const fn value(self) -> i16 {
        self as i16
    }

    pub fn from_i16(value: i16) -> Option<Self> {
        match value {
            0 => Some(ConnectorType::Mysql),
            1 => Some(ConnectorType::Postgres),
            2 => Some(ConnectorType::Mongodb),
            3 => Some(ConnectorType::Sqlserver),
            4 => Some(ConnectorType::Oracle),
            5 => Some(ConnectorType::Db2),
            6 => Some(ConnectorType::Vitess),
            _ => None,
        }
    }
}

#[cfg(feature = "server")]
impl sea_orm::TryGetable for ConnectorType {
    fn try_get(
        res: &sea_orm::QueryResult,
        pre: &str,
        col: &str,
    ) -> Result<Self, sea_orm::TryGetError> {
        let val: i16 = res.try_get(pre, col)?;
        Ok(val.into())
    }

    fn try_get_by<I: sea_orm::ColIdx>(
        res: &sea_orm::QueryResult,
        index: I,
    ) -> Result<Self, sea_orm::TryGetError> {
        let val: i16 = res.try_get_by(index)?;
        Ok(val.into())
    }
}

#[cfg(feature = "server")]
impl sea_orm::sea_query::ValueType for ConnectorType {
    fn try_from(v: sea_orm::sea_query::Value) -> Result<Self, sea_orm::sea_query::ValueTypeErr> {
        match v {
            sea_orm::sea_query::Value::TinyInt(Some(v)) => Ok(ConnectorType::from_i16(v as i16).unwrap_or_default()),
            sea_orm::sea_query::Value::SmallInt(Some(v)) => Ok(ConnectorType::from_i16(v).unwrap_or_default()),
            _ => Err(sea_orm::sea_query::ValueTypeErr),
        }
    }

    fn type_name() -> String {
        stringify!(ConnectorType).to_owned()
    }

    fn array_type() -> sea_orm::sea_query::ArrayType {
        sea_orm::sea_query::ArrayType::SmallInt
    }

    fn column_type() -> sea_orm::sea_query::ColumnType {
        sea_orm::sea_query::ColumnType::SmallInteger
    }
}