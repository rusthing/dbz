use robotech::macros::dao;

#[dao(
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
pub struct DbzSourceConnectorDao;
