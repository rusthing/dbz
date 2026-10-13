use robotech::macros::dao;

#[dao(
    unique_keys: [
        ("name", "实例名称"),
        ("source_connector_name", "上游连接器名称"),
        ("sink_connector_name", "下游连接器名称"),
    ],
    like_columns: [
        Column::Name,
        Column::SourceConnectorName,
        Column::SinkConnectorName,
        Column::Remark,
    ],
)]
pub struct DbzInstanceDao;