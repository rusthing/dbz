//! # Kafka Connect 工具
//!
//! 封装对 Kafka Connect REST API 的调用，提供连接器（Connector）的部署（注册/更新）、
//! 删除以及状态查询能力，供 Debezium 实例的业务层复用。
//!
//! 涉及的 Kafka Connect 接口：
//! - `GET  /connectors`                 连通性探测
//! - `PUT  /connectors/{name}/config`   注册或更新连接器
//! - `DELETE /connectors/{name}`        删除连接器
//! - `GET  /connectors/{name}/status`   查询连接器运行状态

use crate::dic::ConnectorStatus;
use anyhow::anyhow;
use serde::Deserialize;
use serde_json::Value;
use std::sync::LazyLock;
use tracing::info;

/// 复用的 HTTP 客户端
static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);

/// # 连接器操作错误
///
/// 用于区分 Kafka Connect 调用失败的具体原因，便于业务层映射为不同的运行状态。
#[derive(Debug)]
pub enum ConnectorError {
    /// 请求失败（网络异常或返回非 2xx 状态码）
    Request(String),
    /// 连接器不存在（HTTP 404）
    NotFound,
    /// 状态响应解析失败
    Parse(String),
}

impl std::fmt::Display for ConnectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectorError::Request(msg) => write!(f, "请求失败: {msg}"),
            ConnectorError::NotFound => write!(f, "连接器不存在"),
            ConnectorError::Parse(msg) => write!(f, "响应解析失败: {msg}"),
        }
    }
}

impl std::error::Error for ConnectorError {}

/// # Kafka Connect 工具
pub struct DbzConnectorUtils;

impl DbzConnectorUtils {
    /// # 探测 Kafka Connect 连通性
    ///
    /// 请求 `GET /connectors`，用于在注册连接器前确认目标 Kafka Connect 可用。
    ///
    /// ## 参数
    /// * `base_url` - Kafka Connect 地址
    ///
    /// ## 返回值
    /// * `Ok(())` - Kafka Connect 可用
    /// * `Err(anyhow::Error)` - 不可用
    pub async fn check_connect(base_url: &str) -> Result<(), anyhow::Error> {
        let url = format!("{}/connectors", base_url.trim_end_matches('/'));
        let resp = HTTP_CLIENT
            .get(&url)
            .send()
            .await
            .map_err(|e| anyhow!("连接 Kafka Connect 失败: {e}"))?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("连接 Kafka Connect 失败: {body}"));
        }
        Ok(())
    }

    /// # 部署（注册或更新）连接器
    ///
    /// 调用 `PUT /connectors/{name}/config`，连接器不存在时创建，已存在时更新配置。
    ///
    /// ## 参数
    /// * `base_url` - Kafka Connect 地址
    /// * `connector_name` - 连接器名称
    /// * `connector_config` - 连接器配置（JSON 字符串）
    ///
    /// ## 返回值
    /// * `Ok(())` - 部署成功
    /// * `Err(ConnectorError)` - 部署失败
    pub async fn deploy_connector(
        base_url: &str,
        connector_name: &str,
        connector_config: &str,
    ) -> Result<(), ConnectorError> {
        let config = parse_config(connector_config)?;
        let url = format!(
            "{}/connectors/{}/config",
            base_url.trim_end_matches('/'),
            connector_name
        );
        let resp = HTTP_CLIENT
            .put(&url)
            .json(&config)
            .send()
            .await
            .map_err(|e| ConnectorError::Request(format!("部署连接器<{connector_name}>失败: {e}")))?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(ConnectorError::Request(format!(
                "部署连接器<{connector_name}>失败: {body}"
            )));
        }
        info!("连接器<{connector_name}>部署成功");
        Ok(())
    }

    /// # 删除连接器
    ///
    /// 调用 `DELETE /connectors/{name}`，连接器不存在（404）时视为删除成功。
    ///
    /// ## 参数
    /// * `base_url` - Kafka Connect 地址
    /// * `connector_name` - 连接器名称
    ///
    /// ## 返回值
    /// * `Ok(())` - 删除成功或连接器本就不存在
    /// * `Err(anyhow::Error)` - 删除失败
    pub async fn delete_connector(
        base_url: &str,
        connector_name: &str,
    ) -> Result<(), anyhow::Error> {
        let url = format!(
            "{}/connectors/{}",
            base_url.trim_end_matches('/'),
            connector_name
        );
        let resp = HTTP_CLIENT
            .delete(&url)
            .send()
            .await
            .map_err(|e| anyhow!("删除连接器<{connector_name}>失败: {e}"))?;
        // 404 表示连接器已不存在，视为删除成功
        if !resp.status().is_success() && resp.status() != reqwest::StatusCode::NOT_FOUND {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("删除连接器<{connector_name}>失败: {body}"));
        }
        info!("连接器<{connector_name}>删除成功");
        Ok(())
    }

    /// # 查询连接器状态
    ///
    /// 调用 `GET /connectors/{name}/status`，将 Kafka Connect 返回的状态字符串
    /// 映射为 [`ConnectorStatus`]。
    ///
    /// ## 参数
    /// * `base_url` - Kafka Connect 地址
    /// * `connector_name` - 连接器名称
    ///
    /// ## 返回值
    /// * `Ok(ConnectorStatus)` - 连接器状态
    /// * `Err(ConnectorError)` - 查询失败，可通过错误类型区分具体原因
    pub async fn fetch_connector_status(
        base_url: &str,
        connector_name: &str,
    ) -> Result<ConnectorStatus, ConnectorError> {
        let url = format!(
            "{}/connectors/{}/status",
            base_url.trim_end_matches('/'),
            connector_name
        );
        let resp = HTTP_CLIENT.get(&url).send().await.map_err(|e| {
            ConnectorError::Request(format!("查询连接器<{connector_name}>状态失败: {e}"))
        })?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(ConnectorError::NotFound);
        }
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(ConnectorError::Request(format!(
                "查询连接器<{connector_name}>状态失败: {body}"
            )));
        }
        let status_resp: ConnectorStatusResponse = resp.json().await.map_err(|e| {
            ConnectorError::Parse(format!("解析连接器<{connector_name}>状态响应失败: {e}"))
        })?;
        Ok(parse_connector_state(&status_resp.connector.state))
    }
}

/// # Kafka Connect 连接器状态响应
#[derive(Debug, Deserialize)]
struct ConnectorStatusResponse {
    /// 连接器状态
    connector: ConnectorState,
}

/// # Kafka Connect 连接器状态
#[derive(Debug, Deserialize)]
struct ConnectorState {
    /// 状态字符串，如 RUNNING / FAILED / PAUSED / UNASSIGNED / DESTROYED
    state: String,
}

/// # 将连接器配置字符串解析为 JSON
fn parse_config(connector_config: &str) -> Result<Value, ConnectorError> {
    serde_json::from_str(connector_config)
        .map_err(|e| ConnectorError::Parse(format!("连接器配置不是合法的 JSON: {e}")))
}

/// # 将 Kafka Connect 状态字符串映射为 [`ConnectorStatus`]
fn parse_connector_state(state: &str) -> ConnectorStatus {
    match state.to_uppercase().as_str() {
        "RUNNING" => ConnectorStatus::Running,
        "FAILED" => ConnectorStatus::Failed,
        "PAUSED" => ConnectorStatus::Paused,
        "DESTROYED" => ConnectorStatus::Destroyed,
        _ => ConnectorStatus::Unassigned,
    }
}
