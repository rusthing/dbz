use robotech::macros::dic;
use strum::{AsRefStr, Display, EnumString};

/// 确定性失败的细分子类，对应 `dbz_dead_letter.failure_subtype`。
///
/// `failure_category` 只说明「可不可以自动重投」；改报文重投只对**报文本身字段不合规**
/// 的确定性问题有意义（例如字段缺失、类型不对、长度超限）。唯一键冲突、外键缺失这类
/// 记录改报文同样可能救回来，但语义上不属于「字段不合规」，因此不放进白名单。
/// 传输类与响应异常类永远不允许修改报文，子类固定为 `Other`。
#[dic(default = Other)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr, Display, EnumString)]
#[strum(serialize_all = "kebab-case")]
pub enum DeadLetterFailureSubtype {
    /// 未细分（唯一键冲突、外键缺失、旧脚本未上报子类等）
    Other = 0,
    /// 字段不合规（必填缺失、类型不符、长度超限等），允许修改报文后重投
    FieldValidation = 1,
}
