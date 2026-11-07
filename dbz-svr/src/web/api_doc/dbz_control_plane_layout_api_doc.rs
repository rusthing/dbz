use robotech::macros::api_doc;

/// # 控制面画布布局接口文档
///
/// 列表、保存、删除三个手写接口。
#[api_doc(list, upsert, remove)]
pub struct DbzControlPlaneLayoutApiDoc;
