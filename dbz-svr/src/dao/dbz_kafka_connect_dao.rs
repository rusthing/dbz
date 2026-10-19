use robotech::macros::dao;

/// # Kafka Connect
///
/// 连接器实际运行的集群。上游连接器与下游连接器都通过 `kafka_connect_id`
/// 引用它（各为 1:N），数据库侧是 `on delete restrict`：还有连接器指向某台
/// 集群时不允许删除该集群。
#[dao(
    unique_keys: [
        ("name", "名称"),
        ("url", "地址"),
    ],
    like_columns: [
        Column::Name,
        Column::Url,
        Column::Remark,
    ],
)]
pub struct DbzKafkaConnectDao;
