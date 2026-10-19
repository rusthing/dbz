use crate::dto::*;
use crate::vo::{DbzKafkaConnectExVo, DbzKafkaConnectVo};
use robotech::macros::feign;
use robotech::micro_svc::FeignApiClient;

#[feign]
pub struct DbzKafkaConnectApiClient {
    client: FeignApiClient,
}

impl DbzKafkaConnectApiClient {
    pub fn new(client: FeignApiClient) -> Self {
        Self { client }
    }
}
