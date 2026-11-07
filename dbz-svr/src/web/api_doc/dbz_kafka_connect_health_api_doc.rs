use robotech::macros::api_doc;

/// # Kafka Connect 健康检查接口文档
///
/// 把手写的 `health` 登记进 OpenAPI；CRUD 的 21 个方法见
/// [`DbzKafkaConnectApiDoc`](crate::web::api_doc::DbzKafkaConnectApiDoc)。
#[api_doc(health)]
pub struct DbzKafkaConnectHealthApiDoc;
