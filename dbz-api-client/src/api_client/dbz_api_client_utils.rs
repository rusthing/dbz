use crate::api_client::dbz_instance_api_client::DbzInstanceApiClient;
use robotech::macros::api_client;

#[api_client]
pub struct DbzApiClient {
    pub instance_client: DbzInstanceApiClient,
}
