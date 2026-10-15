use robotech::macros::dic;
use strum::{AsRefStr, Display, EnumString};

#[dic]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr, Display, EnumString)]
#[strum(serialize_all = "kebab-case")]
pub enum InstanceRunStatus {
    /// 无状态（刚添加记录）
    None = 0,
    /// 运行中
    Running = 1,
    /// 已禁用
    Disabled = 2,
    /// 连接 KafkaConnect 失败
    KafkaConnectFailed = 3,
    /// 注册下游连接器失败
    RegisterSinkFailed = 4,
    /// 检测下游连接器失败
    CheckSinkFailed = 5,
    /// 检测下游连接器异常
    CheckSinkError = 6,
    /// 检测下游连接器状态失败
    CheckSinkStatusFailed = 7,
    /// 检测下游连接器状态异常
    CheckSinkStatusError = 8,
    /// 注册上游连接器失败
    RegisterSourceFailed = 9,
    /// 检测上游连接器失败
    CheckSourceFailed = 10,
    /// 检测上游连接器异常
    CheckSourceError = 11,
    /// 检测上游连接器状态失败
    CheckSourceStatusFailed = 12,
    /// 检测上游连接器状态异常
    CheckSourceStatusError = 13,
}

impl Default for InstanceRunStatus {
    fn default() -> Self {
        Self::None
    }
}

impl From<InstanceRunStatus> for i16 {
    fn from(val: InstanceRunStatus) -> Self {
        val as i16
    }
}

impl From<i16> for InstanceRunStatus {
    fn from(value: i16) -> Self {
        Self::from_i16(value).unwrap_or_default()
    }
}

impl From<InstanceRunStatus> for Option<i16> {
    fn from(val: InstanceRunStatus) -> Self {
        Some(val as i16)
    }
}

impl From<Option<i16>> for InstanceRunStatus {
    fn from(value: Option<i16>) -> Self {
        match value {
            Some(v) => v.into(),
            None => InstanceRunStatus::default(),
        }
    }
}

impl InstanceRunStatus {
    pub const fn value(self) -> i16 {
        self as i16
    }

    pub fn from_i16(value: i16) -> Option<Self> {
        match value {
            0 => Some(InstanceRunStatus::None),
            1 => Some(InstanceRunStatus::Running),
            2 => Some(InstanceRunStatus::Disabled),
            3 => Some(InstanceRunStatus::KafkaConnectFailed),
            4 => Some(InstanceRunStatus::RegisterSinkFailed),
            5 => Some(InstanceRunStatus::CheckSinkFailed),
            6 => Some(InstanceRunStatus::CheckSinkError),
            7 => Some(InstanceRunStatus::CheckSinkStatusFailed),
            8 => Some(InstanceRunStatus::CheckSinkStatusError),
            9 => Some(InstanceRunStatus::RegisterSourceFailed),
            10 => Some(InstanceRunStatus::CheckSourceFailed),
            11 => Some(InstanceRunStatus::CheckSourceError),
            12 => Some(InstanceRunStatus::CheckSourceStatusFailed),
            13 => Some(InstanceRunStatus::CheckSourceStatusError),
            _ => None,
        }
    }
}

#[cfg(feature = "server")]
impl sea_orm::TryGetable for InstanceRunStatus {
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
impl sea_orm::sea_query::ValueType for InstanceRunStatus {
    fn try_from(v: sea_orm::sea_query::Value) -> Result<Self, sea_orm::sea_query::ValueTypeErr> {
        match v {
            sea_orm::sea_query::Value::TinyInt(Some(v)) => Ok(InstanceRunStatus::from_i16(v as i16).unwrap_or_default()),
            sea_orm::sea_query::Value::SmallInt(Some(v)) => Ok(InstanceRunStatus::from_i16(v).unwrap_or_default()),
            _ => Err(sea_orm::sea_query::ValueTypeErr),
        }
    }

    fn type_name() -> String {
        stringify!(InstanceRunStatus).to_owned()
    }

    fn array_type() -> sea_orm::sea_query::ArrayType {
        sea_orm::sea_query::ArrayType::SmallInt
    }

    fn column_type() -> sea_orm::sea_query::ColumnType {
        sea_orm::sea_query::ColumnType::SmallInteger.into()
    }
}