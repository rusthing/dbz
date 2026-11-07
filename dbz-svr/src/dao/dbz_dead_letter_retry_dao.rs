use robotech::macros::dao;

/// # 死信重试队列
///
/// 幂等键与死信表一致，由服务层手写条件查重；`next_at_ms` 上有索引，worker 按到期
/// 时间取行。
#[dao]
pub struct DbzDeadLetterRetryDao;
