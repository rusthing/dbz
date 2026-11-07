use robotech::macros::dic;
use strum::{AsRefStr, Display, EnumString};

/// 死信记录的处理状态，对应 `dbz_dead_letter.status`。
///
/// 只有「待处理」计入前端角标；重投成功、人工废弃、人工处理过的记录保留在表里
/// 供追溯，不再提醒。
#[dic(default = Pending)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr, Display, EnumString)]
#[strum(serialize_all = "kebab-case")]
pub enum DeadLetterStatus {
    /// 待处理
    Pending = 0,
    /// 已重投（重新 POST 到 magic 并成功）
    Retried = 1,
    /// 已废弃（人工确认不再重投）
    Discarded = 2,
    /// 已人工处理（在别处修好后标记）
    ManualHandled = 3,
}
