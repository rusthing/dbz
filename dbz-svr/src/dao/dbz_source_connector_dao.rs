use robotech::macros::dao;

/// # 上游连接器
///
/// 通过 `kafka_connect_id` 指向运行它的 Kafka Connect 集群，本身不再保存地址。
#[dao(
    foreign_keys: [
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
pub struct DbzSourceConnectorDao;
