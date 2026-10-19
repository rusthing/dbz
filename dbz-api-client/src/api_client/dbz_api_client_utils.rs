use crate::api_client::dbz_kafka_connect_api_client::DbzKafkaConnectApiClient;
use crate::api_client::dbz_sink_connector_api_client::DbzSinkConnectorApiClient;
use crate::api_client::dbz_source_connector_api_client::DbzSourceConnectorApiClient;
use robotech::macros::api_client;

#[api_client]
pub struct DbzApiClient {
    pub kafka_connect: DbzKafkaConnectApiClient,
    pub source_connector: DbzSourceConnectorApiClient,
    pub sink_connector: DbzSinkConnectorApiClient,
}
