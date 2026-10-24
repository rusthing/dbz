use robotech::macros::dao;

/// # Kafka 集群（broker 侧）
///
/// 上游连接器通过 `dbz_kafka_connect.kafka_broker_id` 间接引用它。数据库侧是
/// `on delete restrict`：还有 Connect 记录指向某个集群时不允许删除该集群 —— 删了
/// 之后那些连接器的信号通道就没了落点。
///
/// `bootstrap_servers` 做唯一键：同一组地址配两遍没有意义，只会让「改哪一条」变得
/// 含糊。
#[dao(
    unique_keys: [
        ("name", "名称"),
        ("bootstrap_servers", "Broker 地址"),
    ],
    like_columns: [
        Column::Name,
        Column::BootstrapServers,
        Column::Remark,
    ],
)]
pub struct DbzKafkaBrokerDao;
