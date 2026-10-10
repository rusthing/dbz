use crate::dto::*;
use crate::vo::{DbzTaskExVo, DbzTaskVo};
use robotech::api::Ro;
use robotech::macros::feign;
use robotech::micro_svc::FeignApiClient;

#[feign]
pub struct DbzTaskApiClient {
    client: FeignApiClient,
}

impl DbzTaskApiClient {
    pub fn new(client: FeignApiClient) -> Self {
        Self { client }
    }

    pub async fn register(
        &self,
        dto: &DbzTaskAddDto,
    ) -> Result<Ro<DbzTaskVo>, robotech::api_client::ApiClientError> {
        let url = "/dbz/task/register".to_string();
        let headers = Self::build_headers(
            dto._current_user_id.value(),
            dto._current_ms.map(|m| m.value()),
        )?;
        self.client
            .request::<DbzTaskAddDto, DbzTaskVo>(
                ::reqwest::Method::POST,
                &url,
                None,
                Some(dto),
                Some(&headers),
            )
            .await
    }

    /// # 注销连接器
    pub async fn unregister(
        &self,
        id: u64,
    ) -> Result<Ro<DbzTaskVo>, robotech::api_client::ApiClientError> {
        let url = format!("/dbz/task/unregister/{id}");
        let headers = Self::build_headers(0, None)?;
        self.client
            .request::<(), DbzTaskVo>(
                ::reqwest::Method::POST,
                &url,
                None,
                None,
                Some(&headers),
            )
            .await
    }

    /// # 查询连接器状态
    pub async fn check_status(
        &self,
        id: u64,
    ) -> Result<Ro<DbzTaskVo>, robotech::api_client::ApiClientError> {
        let url = format!("/dbz/task/check-status/{id}");
        let headers = Self::build_headers(0, None)?;
        self.client
            .request::<(), DbzTaskVo>(
                ::reqwest::Method::POST,
                &url,
                None,
                None,
                Some(&headers),
            )
            .await
    }

    /// # 重启连接器
    pub async fn restart(
        &self,
        id: u64,
    ) -> Result<Ro<DbzTaskVo>, robotech::api_client::ApiClientError> {
        let url = format!("/dbz/task/restart/{id}");
        let headers = Self::build_headers(0, None)?;
        self.client
            .request::<(), DbzTaskVo>(
                ::reqwest::Method::POST,
                &url,
                None,
                None,
                Some(&headers),
            )
            .await
    }
}