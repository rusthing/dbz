use robotech::macros::api_doc;

/// # 死信接口文档
///
/// 同步、分页、计数、重投、保存待投报文、废弃、标记/撤销人工处理，八个手写接口。
/// `#[api_doc]` 少写一个方法不会报错，但 swagger 里会缺，改路由时两边一起动。
#[api_doc(
    sync,
    page,
    pending_count,
    retry,
    edit_payload,
    discard,
    mark_handled,
    unmark_handled
)]
pub struct DbzDeadLetterApiDoc;
