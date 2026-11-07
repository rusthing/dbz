use robotech::macros::api_doc;

/// # 死信接口文档
///
/// 上报、分页、计数、重投、保存待投报文、废弃六个手写接口。
#[api_doc(report, page, pending_count, retry, edit_payload, discard)]
pub struct DbzDeadLetterApiDoc;
