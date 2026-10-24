use robotech::macros::api_doc;

/// # Kafka 连接器接口文档
///
/// 把 ctrl 里手写的运维接口登记进 OpenAPI。`#[api_doc]` 按结构体名推导
/// ctrl 模块（`dbz_kafka_connector_ctrl`）与文档地址
/// `/dbz-kafka-connector/openapi.json`，所以结构体名不能省掉 `Dbz` 前缀。
#[api_doc(
    list_connector_plugins,
    list_connectors,
    restart_connector,
    restart_connector_task,
    stop_connector,
    resume_connector,
    reset_connector_offsets,
    delete_connector
)]
pub struct DbzKafkaConnectorApiDoc;
