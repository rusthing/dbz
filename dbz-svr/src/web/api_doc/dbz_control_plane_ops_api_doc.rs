use robotech::macros::api_doc;

/// # 控制面操作接口文档
///
/// 折点读写、节点删除、双向配置同步与单条连接器一致性检测都是手写接口，
/// 统一登记在这里；布局列表/保存/删除见
/// [`DbzControlPlaneLayoutApiDoc`](crate::web::api_doc::DbzControlPlaneLayoutApiDoc)。
#[api_doc(
    list_edges,
    upsert_edge,
    remove_edge,
    delete_node,
    sync_connector_config,
    detect_connector
)]
pub struct DbzControlPlaneOpsApiDoc;
