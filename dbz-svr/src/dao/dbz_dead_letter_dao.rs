use robotech::macros::dao;

/// # 死信记录
///
/// 幂等键是复合键 `(connector_type, connector_id, topic, record_key)`，`#[dao]` 的
/// `unique_keys` 只表达单列唯一，这里不声明，由服务层手写条件查重。
#[dao]
pub struct DbzDeadLetterDao;
