use crate::api_client::dbz_task_api_client::DbzTaskApiClient;
use robotech::macros::api_client;

#[api_client]
pub struct DbzApiClient {
    pub task_client: DbzTaskApiClient,
}