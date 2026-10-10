use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display, EnumString};
use utoipa::ToSchema;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    AsRefStr,
    Display,
    EnumString,
    Serialize,
    Deserialize,
    ToSchema,
)]
#[strum(serialize_all = "kebab-case")]
pub enum ConnectorStatus {
    /// 运行中
    Running = 0,
    /// 已失败
    Failed = 1,
    /// 已暂停
    Paused = 2,
    /// 未分配
    Unassigned = 3,
    /// 已销毁
    Destroyed = 4,
}

impl Default for ConnectorStatus {
    fn default() -> Self {
        Self::Unassigned
    }
}

impl From<ConnectorStatus> for i16 {
    fn from(val: ConnectorStatus) -> Self {
        val as i16
    }
}

impl From<i16> for ConnectorStatus {
    fn from(value: i16) -> Self {
        Self::from_i16(value).unwrap_or_default()
    }
}

impl ConnectorStatus {
    pub const fn value(self) -> i16 {
        self as i16
    }

    pub fn from_i16(value: i16) -> Option<Self> {
        match value {
            0 => Some(ConnectorStatus::Running),
            1 => Some(ConnectorStatus::Failed),
            2 => Some(ConnectorStatus::Paused),
            3 => Some(ConnectorStatus::Unassigned),
            4 => Some(ConnectorStatus::Destroyed),
            _ => None,
        }
    }
}

#[cfg(feature = "server")]
impl sea_orm::TryGetable for ConnectorStatus {
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
impl sea_orm::sea_query::ValueType for ConnectorStatus {
    fn try_from(v: sea_orm::sea_query::Value) -> Result<Self, sea_orm::sea_query::ValueTypeErr> {
        match v {
            sea_orm::sea_query::Value::TinyInt(Some(v)) => Ok(ConnectorStatus::from_i16(v as i16).unwrap_or_default()),
            sea_orm::sea_query::Value::SmallInt(Some(v)) => Ok(ConnectorStatus::from_i16(v).unwrap_or_default()),
            _ => Err(sea_orm::sea_query::ValueTypeErr),
        }
    }

    fn type_name() -> String {
        stringify!(ConnectorStatus).to_owned()
    }

    fn array_type() -> sea_orm::sea_query::ArrayType {
        sea_orm::sea_query::ArrayType::SmallInt
    }

    fn column_type() -> sea_orm::sea_query::ColumnType {
        sea_orm::sea_query::ColumnType::SmallInteger
    }
}