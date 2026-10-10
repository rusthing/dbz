use crate::dto::*;
use crate::vo::{DbzInstanceExVo, DbzInstanceVo};
use robotech::macros::feign;
use robotech::micro_svc::FeignApiClient;

#[feign]
pub struct DbzInstanceApiClient {
    client: FeignApiClient,
}

impl DbzInstanceApiClient {
    pub fn new(client: FeignApiClient) -> Self {
        Self { client }
    }
}