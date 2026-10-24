use robotech::macros::api_doc;

/// # 上游连接器再同步接口文档
///
/// 把手写的两个快照接口登记进 OpenAPI。`#[api_doc]` 按结构体名推导 ctrl 模块
/// （`dbz_source_connector_snapshot_ctrl`）与文档地址
/// `/dbz-source-connector-snapshot/openapi.json`。
#[api_doc(preview_snapshot, execute_snapshot)]
pub struct DbzSourceConnectorSnapshotApiDoc;
