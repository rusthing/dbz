use crate::dto::*;
use crate::vo::{DbzSourceConnectorExVo, DbzSourceConnectorVo};
use robotech::macros::feign;
use robotech::micro_svc::FeignApiClient;

#[feign]
pub struct DbzSourceConnectorApiClient {
    client: FeignApiClient,
}

impl DbzSourceConnectorApiClient {
    pub fn new(client: FeignApiClient) -> Self {
        Self { client }
    }
}
