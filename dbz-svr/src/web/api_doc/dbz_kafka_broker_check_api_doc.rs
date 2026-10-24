use robotech::macros::api_doc;

/// # Kafka 集群检测接口文档
///
/// 把手写的 `check_broker` 登记进 OpenAPI；CRUD 的 21 个方法见
/// [`DbzKafkaBrokerApiDoc`](crate::web::api_doc::DbzKafkaBrokerApiDoc)。
#[api_doc(check_broker)]
pub struct DbzKafkaBrokerCheckApiDoc;
