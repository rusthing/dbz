use robotech::macros::api_doc;

/// # Kafka Connect 检测接口文档
///
/// 把手写的 `detect` 登记进 OpenAPI；CRUD 的 21 个方法见
/// [`DbzKafkaConnectApiDoc`](crate::web::api_doc::DbzKafkaConnectApiDoc)。
#[api_doc(detect)]
pub struct DbzKafkaConnectDetectApiDoc;
