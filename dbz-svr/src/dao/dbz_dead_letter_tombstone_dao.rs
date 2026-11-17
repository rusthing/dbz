use robotech::macros::dao;

/// # 死信清理墓碑
///
/// 只由清理流程批量写入（一次一个 `PURGE_BATCH`），同步时按唯一键查一条；没有独立的
/// 增删改查入口。唯一键 `(connector_name, connector_id, topic, partition, offset)` 由
/// 批量 upsert 保证，`#[dao]` 的 `unique_keys` 只表达单列唯一，这里不声明。
#[dao]
pub struct DbzDeadLetterTombstoneDao;
