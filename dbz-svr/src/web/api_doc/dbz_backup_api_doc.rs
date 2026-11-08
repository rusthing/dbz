use robotech::macros::api_doc;

/// # Kafka 集群备份 / 恢复接口文档
///
/// 把手写的 `export` / `restore` 登记进 OpenAPI。
#[api_doc(export, restore)]
pub struct DbzBackupApiDoc;
