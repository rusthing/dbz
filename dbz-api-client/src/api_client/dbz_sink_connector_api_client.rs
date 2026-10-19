use crate::dto::*;
use crate::vo::{DbzSinkConnectorExVo, DbzSinkConnectorVo};
use robotech::macros::feign;
use robotech::micro_svc::FeignApiClient;

#[feign]
pub struct DbzSinkConnectorApiClient {
    client: FeignApiClient,
}

impl DbzSinkConnectorApiClient {
    pub fn new(client: FeignApiClient) -> Self {
        Self { client }
    }
}
