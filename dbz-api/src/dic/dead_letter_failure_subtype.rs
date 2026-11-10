use robotech::macros::dic;
use strum::{AsRefStr, Display, EnumString};

/// 异常细分子类，对应 `dbz_dead_letter.failure_subtype`。
///
/// 由 DLQ 异常消息里的字段校验关键词推导。只有「数据/格式类 + 字段不合规」允许
/// 修改报文后重投（写回源 topic）；投递类、其它类异常改报文没有意义，子类固定为
/// `Other`。
#[dic(default = Other)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr, Display, EnumString)]
#[strum(serialize_all = "kebab-case")]
pub enum DeadLetterFailureSubtype {
    /// 未细分（异常消息里没有命中字段校验关键词）
    Other = 0,
    /// 字段不合规（必填缺失、类型不符、长度超限等），允许修改报文后重投
    FieldValidation = 1,
}
