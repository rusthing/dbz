use robotech::macros::dao;

/// # 控制面画布连线折点
///
/// 唯一键是 `edge_id`，`#[dao]` 的 `unique_keys` 只表达单列唯一，这里由服务层
/// 按 `edge_id` 查询后更新，不依赖宏生成的唯一键。
#[dao]
pub struct DbzControlPlaneEdgeLayoutDao;
