use robotech::macros::dao;

/// # 下游连接器
///
/// 通过 `source_connector_id` 归属上游连接器（`dbz_source_connector`），
/// 数据库侧是 `on delete restrict`：上游下还有下游时不允许删除上游。
#[dao(
    foreign_keys: [
        ("source_connector_id", "dbz_source_connector", "上游连接器"),
    ],
    unique_keys: [
        ("name", "名称"),
        ("connector_name", "连接器名称"),
    ],
    like_columns: [
        Column::Name,
        Column::ConnectorName,
        Column::Remark,
    ],
)]
pub struct DbzSinkConnectorDao;
