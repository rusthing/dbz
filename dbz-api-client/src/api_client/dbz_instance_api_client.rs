use crate::dto::*;
use crate::vo::{DbzInstanceExVo, DbzInstanceVo};
use robotech::api::Ro;
use robotech::api_client::ApiClientError;
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

    /// # 注册实例
    ///
    /// 创建实例记录、jobx 定时检查任务计划，并向 Kafka Connect 注册上下游连接器。
    ///
    /// ## 参数
    /// * `dto` - 包含实例信息的传输对象（`jobId` 无需传入）
    ///
    /// ## 返回值
    /// * `Ok(Ro<DbzInstanceVo>)` - 注册后的实例信息
    /// * `Err(ApiClientError)` - 请求失败
    pub async fn register(
        &self,
        dto: &DbzInstanceAddDto,
    ) -> Result<Ro<DbzInstanceVo>, ApiClientError> {
        let url = "/dbz/instance/register";
        let headers = Self::build_headers(
            dto._current_user_id.value(),
            dto._current_ms.map(|m| m.value()),
        )?;
        self.client
            .request::<DbzInstanceAddDto, DbzInstanceVo>(
                reqwest::Method::POST,
                url,
                None,
                Some(dto),
                Some(&headers),
            )
            .await
    }

    /// # 注销实例
    ///
    /// 删除 Kafka Connect 上下游连接器、jobx 定时检查任务计划及实例记录。
    ///
    /// ## 参数
    /// * `id` - 实例ID
    /// * `current_user_id` - 当前操作用户ID
    ///
    /// ## 返回值
    /// * `Ok(Ro<DbzInstanceVo>)` - 被注销的实例信息
    /// * `Err(ApiClientError)` - 请求失败
    pub async fn unregister(
        &self,
        id: u64,
        current_user_id: u64,
    ) -> Result<Ro<DbzInstanceVo>, ApiClientError> {
        let url = format!("/dbz/instance/unregister/{id}");
        let headers = Self::build_headers(current_user_id, None)?;
        self.client
            .request::<(), DbzInstanceVo>(
                reqwest::Method::POST,
                &url,
                None,
                None::<&()>,
                Some(&headers),
            )
            .await
    }

    /// # 查询实例连接器状态
    ///
    /// ## 参数
    /// * `id` - 实例ID
    /// * `current_user_id` - 当前操作用户ID
    ///
    /// ## 返回值
    /// * `Ok(Ro<DbzInstanceVo>)` - 更新后的实例信息
    /// * `Err(ApiClientError)` - 请求失败
    pub async fn check_status(
        &self,
        id: u64,
        current_user_id: u64,
    ) -> Result<Ro<DbzInstanceVo>, ApiClientError> {
        let url = format!("/dbz/instance/check-status/{id}");
        let headers = Self::build_headers(current_user_id, None)?;
        self.client
            .request::<(), DbzInstanceVo>(
                reqwest::Method::POST,
                &url,
                None,
                None::<&()>,
                Some(&headers),
            )
            .await
    }

    /// # 重启实例连接器
    ///
    /// ## 参数
    /// * `id` - 实例ID
    /// * `current_user_id` - 当前操作用户ID
    ///
    /// ## 返回值
    /// * `Ok(Ro<DbzInstanceVo>)` - 更新后的实例信息
    /// * `Err(ApiClientError)` - 请求失败
    pub async fn restart(
        &self,
        id: u64,
        current_user_id: u64,
    ) -> Result<Ro<DbzInstanceVo>, ApiClientError> {
        let url = format!("/dbz/instance/restart/{id}");
        let headers = Self::build_headers(current_user_id, None)?;
        self.client
            .request::<(), DbzInstanceVo>(
                reqwest::Method::POST,
                &url,
                None,
                None::<&()>,
                Some(&headers),
            )
            .await
    }
}
