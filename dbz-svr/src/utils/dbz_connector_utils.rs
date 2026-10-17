//! # Kafka Connect 工具
//!
//! 封装对 Kafka Connect REST API 的调用，提供连接器（Connector）的部署（注册/更新）、
//! 删除以及状态查询能力，供 Debezium 实例的业务层复用。
//!
//! 涉及的 Kafka Connect 接口：
//! - `GET  /connectors`                 连通性探测
//! - `GET  /connectors?expand=...`      查看所有连接器（含任务状态）
//! - `GET  /connector-plugins`          查看已安装的连接器插件
//! - `PUT  /connectors/{name}/config`   注册或更新连接器
//! - `DELETE /connectors/{name}`        删除连接器
//! - `GET  /connectors/{name}/status`   查询连接器运行状态
//! - `POST /connectors/{name}/restart`  重启连接器（可连同其任务）
//! - `POST /connectors/{name}/tasks/{id}/restart` 重启单个任务

use crate::dic::ConnectorStatus;
use crate::vo::{ConnectorPluginVo, ConnectorTaskVo, ConnectorVo};
use anyhow::anyhow;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::LazyLock;
use tracing::{info, warn};

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
            .map_err(|e| {
                ConnectorError::Request(format!("部署连接器<{connector_name}>失败: {e}"))
            })?;
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

    /// # 检查连接器是否存在
    ///
    /// 调用 `GET /connectors/{name}`，根据返回状态码判断连接器是否存在。
    /// 与 [`fetch_connector_status`] 不同，本方法仅判断存在性，不做状态解析。
    ///
    /// ## 参数
    /// * `base_url` - Kafka Connect 地址
    /// * `connector_name` - 连接器名称
    ///
    /// ## 返回值
    /// * `Ok(true)` - 连接器存在
    /// * `Ok(false)` - 连接器不存在
    /// * `Err(ConnectorError)` - 请求失败
    pub async fn connector_exists(
        base_url: &str,
        connector_name: &str,
    ) -> Result<bool, ConnectorError> {
        let url = format!(
            "{}/connectors/{}",
            base_url.trim_end_matches('/'),
            connector_name
        );
        let resp = HTTP_CLIENT.get(&url).send().await.map_err(|e| {
            ConnectorError::Request(format!("检查连接器<{connector_name}>是否存在失败: {e}"))
        })?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(false);
        }
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(ConnectorError::Request(format!(
                "检查连接器<{connector_name}>是否存在失败: {body}"
            )));
        }
        Ok(true)
    }

    /// # 查看连接器插件
    ///
    /// 调用 `GET /connector-plugins`，返回 Kafka Connect 已安装的全部连接器插件。
    /// 常用于在新建实例前确认目标环境是否已安装对应的 Debezium 插件。
    ///
    /// ## 参数
    /// * `base_url` - Kafka Connect 地址
    ///
    /// ## 返回值
    /// * `Ok(Vec<ConnectorPluginVo>)` - 插件列表，未安装任何插件时为空 Vec
    /// * `Err(ConnectorError)` - 请求失败或响应解析失败
    pub async fn list_connector_plugins(
        base_url: &str,
    ) -> Result<Vec<ConnectorPluginVo>, ConnectorError> {
        let url = format!("{}/connector-plugins", base_url.trim_end_matches('/'));
        let resp = HTTP_CLIENT
            .get(&url)
            .send()
            .await
            .map_err(|e| ConnectorError::Request(format!("查询连接器插件失败: {e}")))?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(ConnectorError::Request(format!(
                "查询连接器插件失败: {body}"
            )));
        }
        let plugins: Vec<ConnectorPluginVo> = resp
            .json()
            .await
            .map_err(|e| ConnectorError::Parse(format!("解析连接器插件响应失败: {e}")))?;
        info!("查询到 {} 个连接器插件", plugins.len());
        Ok(plugins)
    }

    /// # 查看所有连接器
    ///
    /// 调用 `GET /connectors?expand=info&expand=status`，一次性取回连接器的名称、
    /// 类名、类型以及连接器与任务的运行状态。
    ///
    /// 若目标 Kafka Connect 版本过低、不支持 `expand` 参数（会忽略该参数并返回
    /// 纯名称数组），则退化为逐个查询状态，此时 `connector_class` / `connector_type`
    /// 为空、`tasks` 为空列表。
    ///
    /// ## 参数
    /// * `base_url` - Kafka Connect 地址
    ///
    /// ## 返回值
    /// * `Ok(Vec<ConnectorVo>)` - 连接器列表，无连接器时为空 Vec
    /// * `Err(ConnectorError)` - 请求失败或响应解析失败
    pub async fn list_connectors(base_url: &str) -> Result<Vec<ConnectorVo>, ConnectorError> {
        let url = format!("{}/connectors", base_url.trim_end_matches('/'));
        let resp = HTTP_CLIENT
            .get(&url)
            .query(&[("expand", "info"), ("expand", "status")])
            .send()
            .await
            .map_err(|e| ConnectorError::Request(format!("查询连接器列表失败: {e}")))?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(ConnectorError::Request(format!(
                "查询连接器列表失败: {body}"
            )));
        }
        let value: Value = resp
            .json()
            .await
            .map_err(|e| ConnectorError::Parse(format!("解析连接器列表响应失败: {e}")))?;

        let connectors = match value {
            // 支持 expand 的版本：{ "<name>": { "info": {...}, "status": {...} } }
            Value::Object(map) => map
                .into_iter()
                .map(|(name, item)| parse_expanded_connector(&name, item))
                .collect::<Result<Vec<_>, _>>()?,
            // 低版本忽略 expand，只返回名称数组
            Value::Array(items) => {
                let mut connectors = Vec::with_capacity(items.len());
                for item in items {
                    let Some(name) = item.as_str() else {
                        continue;
                    };
                    let state = Self::fetch_connector_status(base_url, name)
                        .await
                        .unwrap_or(ConnectorStatus::Unassigned);
                    connectors.push(ConnectorVo {
                        name: name.to_string(),
                        connector_class: String::new(),
                        connector_type: String::new(),
                        state,
                        tasks: Vec::new(),
                    });
                }
                warn!("Kafka Connect 未返回 expand 信息，连接器列表已退化为仅含名称与状态");
                connectors
            }
            _ => {
                return Err(ConnectorError::Parse(
                    "连接器列表响应格式不符合预期".to_string(),
                ));
            }
        };

        info!("查询到 {} 个连接器", connectors.len());
        Ok(connectors)
    }

    /// # 重启连接器
    ///
    /// 调用 `POST /connectors/{name}/restart`。Kafka Connect 的重启是**异步**的，
    /// 接口返回成功只代表重启请求已被接受，实际状态需稍后通过
    /// [`list_connectors`](Self::list_connectors) 或
    /// [`fetch_connector_status`](Self::fetch_connector_status) 复查。
    ///
    /// ## 参数
    /// * `base_url` - Kafka Connect 地址
    /// * `connector_name` - 连接器名称
    /// * `include_tasks` - 是否同时重启该连接器的任务
    /// * `only_failed` - 是否只重启失败的任务（仅在 `include_tasks = true` 时有意义）
    ///
    /// ## 返回值
    /// * `Ok(())` - 重启请求已被接受
    /// * `Err(ConnectorError)` - 连接器不存在或请求失败
    pub async fn restart_connector(
        base_url: &str,
        connector_name: &str,
        include_tasks: bool,
        only_failed: bool,
    ) -> Result<(), ConnectorError> {
        let url = format!(
            "{}/connectors/{}/restart",
            base_url.trim_end_matches('/'),
            connector_name
        );
        let resp = HTTP_CLIENT
            .post(&url)
            .query(&[("includeTasks", include_tasks), ("onlyFailed", only_failed)])
            .send()
            .await
            .map_err(|e| {
                ConnectorError::Request(format!("重启连接器<{connector_name}>失败: {e}"))
            })?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(ConnectorError::NotFound);
        }
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(ConnectorError::Request(format!(
                "重启连接器<{connector_name}>失败: {body}"
            )));
        }
        info!(
            "连接器<{connector_name}>重启请求已接受(includeTasks={include_tasks}, onlyFailed={only_failed})"
        );
        Ok(())
    }

    /// # 重启连接器的单个任务
    ///
    /// 调用 `POST /connectors/{name}/tasks/{task}/restart`。与
    /// [`restart_connector`](Self::restart_connector) 一样是异步的。
    ///
    /// ## 参数
    /// * `base_url` - Kafka Connect 地址
    /// * `connector_name` - 连接器名称
    /// * `task_id` - 任务ID
    ///
    /// ## 返回值
    /// * `Ok(())` - 重启请求已被接受
    /// * `Err(ConnectorError)` - 连接器或任务不存在、请求失败
    pub async fn restart_connector_task(
        base_url: &str,
        connector_name: &str,
        task_id: u32,
    ) -> Result<(), ConnectorError> {
        let url = format!(
            "{}/connectors/{}/tasks/{}/restart",
            base_url.trim_end_matches('/'),
            connector_name,
            task_id
        );
        let resp = HTTP_CLIENT.post(&url).send().await.map_err(|e| {
            ConnectorError::Request(format!(
                "重启连接器<{connector_name}>的任务<{task_id}>失败: {e}"
            ))
        })?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(ConnectorError::NotFound);
        }
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(ConnectorError::Request(format!(
                "重启连接器<{connector_name}>的任务<{task_id}>失败: {body}"
            )));
        }
        info!("连接器<{connector_name}>的任务<{task_id}>重启请求已接受");
        Ok(())
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
    #[serde(default)]
    state: String,
}

/// # Kafka Connect 连接器的 expand 信息
///
/// 对应 `GET /connectors?expand=info&expand=status` 中每个连接器的值。
#[derive(Debug, Deserialize)]
struct ExpandedConnector {
    /// `expand=info` 的部分
    #[serde(default)]
    info: Option<ConnectorInfo>,
    /// `expand=status` 的部分
    #[serde(default)]
    status: Option<ConnectorStatusDetail>,
}

/// # 连接器基本信息
#[derive(Debug, Deserialize)]
struct ConnectorInfo {
    /// 连接器配置
    #[serde(default)]
    config: HashMap<String, String>,
    /// 连接器类型：source / sink
    #[serde(rename = "type", default)]
    connector_type: Option<String>,
}

/// # 连接器状态详情
#[derive(Debug, Deserialize)]
struct ConnectorStatusDetail {
    /// 连接器自身状态
    #[serde(default)]
    connector: Option<ConnectorState>,
    /// 任务状态列表
    #[serde(default)]
    tasks: Vec<ConnectorTaskState>,
}

/// # 连接器任务状态
#[derive(Debug, Deserialize)]
struct ConnectorTaskState {
    /// 任务ID
    #[serde(default)]
    id: u32,
    /// 任务状态字符串
    #[serde(default)]
    state: String,
    /// 执行该任务的 worker
    #[serde(default)]
    worker_id: Option<String>,
    /// 异常堆栈
    #[serde(default)]
    trace: Option<String>,
}

/// # 将 expand 响应中的单个连接器转换为 [`ConnectorVo`]
fn parse_expanded_connector(name: &str, value: Value) -> Result<ConnectorVo, ConnectorError> {
    let expanded: ExpandedConnector = serde_json::from_value(value)
        .map_err(|e| ConnectorError::Parse(format!("解析连接器<{name}>信息失败: {e}")))?;

    let connector_class = expanded
        .info
        .as_ref()
        .and_then(|info| info.config.get("connector.class").cloned())
        .unwrap_or_default();
    let connector_type = expanded
        .info
        .as_ref()
        .and_then(|info| info.connector_type.clone())
        .unwrap_or_default();
    let state = expanded
        .status
        .as_ref()
        .and_then(|status| status.connector.as_ref())
        .map(|connector| parse_connector_state(&connector.state))
        .unwrap_or(ConnectorStatus::Unassigned);
    let tasks = expanded
        .status
        .map(|status| {
            status
                .tasks
                .into_iter()
                .map(|task| ConnectorTaskVo {
                    id: task.id,
                    state: parse_connector_state(&task.state),
                    worker_id: task.worker_id,
                    trace: task.trace,
                })
                .collect()
        })
        .unwrap_or_default();

    Ok(ConnectorVo {
        name: name.to_string(),
        connector_class,
        connector_type,
        state,
        tasks,
    })
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
