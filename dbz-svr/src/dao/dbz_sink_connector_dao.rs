use robotech::macros::dao;

/// # 下游连接器
///
/// 通过 `source_connector_id` 归属上游连接器（`dbz_source_connector`），通过
/// `kafka_connect_id` 指向运行它的 Kafka Connect 集群。两个外键在数据库侧都是
/// `on delete restrict`。
#[dao(
    foreign_keys: [
        ("source_connector_id", "dbz_source_connector", "上游连接器"),
        ("kafka_connect_id", "dbz_kafka_connect", "Kafka Connect"),
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
