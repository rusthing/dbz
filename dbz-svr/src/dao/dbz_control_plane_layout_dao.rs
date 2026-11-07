use robotech::macros::dao;

/// # 控制面画布节点位置
///
/// 唯一键是 `(node_type, node_id)`，`#[dao]` 的 `unique_keys` 只表达单列唯一，
/// 这里不声明，由服务层按类型 + ID 查询。
#[dao]
pub struct DbzControlPlaneLayoutDao;
