use robotech::macros::dao;

/// # 死信记录（Kafka Connect DLQ 镜像）
///
/// 幂等键是 `(connector_id, topic, partition, offset)`：同一源 topic 可能被多条
/// sink 连接器消费，带上连接器才能区分各自的 DLQ 记录。`#[dao]` 的 `unique_keys`
/// 只表达单列唯一，这里不声明，由服务层手写条件查重。
#[dao]
pub struct DbzDeadLetterDao;
