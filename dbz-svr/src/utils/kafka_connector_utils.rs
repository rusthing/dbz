//! # Kafka 连接器工具（Kafka Connect REST）
//!
//! 封装对 Kafka Connect REST API 的调用，提供连接器（Connector）的部署（注册/更新）、
//! 删除以及状态查询能力，供上层业务复用。
//!
//! 命名上 `kafka_` 指「远端集群上的连接器」，与库记录 `dbz_*` 区分：本模块不发 SQL、
//! 只发 HTTP。
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
//! - `PUT  /connectors/{name}/stop`     停止连接器（Kafka 3.5+，保留配置与位点）
//! - `PUT  /connectors/{name}/resume`   恢复运行
//! - `DELETE /connectors/{name}/offsets` 清空位点（要求连接器先处于停止状态）

use crate::dic::KafkaConnectorStatus;
use crate::utils::trace_head;
use crate::vo::{KafkaConnectorPluginVo, KafkaConnectorTaskVo, KafkaConnectorVo};
use anyhow::anyhow;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::LazyLock;
use tracing::{info, warn};

/// 复用的 HTTP 客户端
static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);

/// 失败原因首行的最大长度
///
/// Debezium 的首行异常通常几十个字，300 足够看出根因；再长就该去集群看完整 trace。
const TRACE_HEAD_MAX: usize = 300;

/// # 一次状态查询的结论
///
/// 状态、要重启的任务、以及「为什么失败」三件事一次带出来：调用方写库、告警、
/// 修复都要用，分三次查询没有意义。
#[derive(Debug, Clone)]
pub struct ConnectorHealth {
    /// 连接器状态（任务全挂时提升为 `Failed`）
    pub state: KafkaConnectorStatus,
    /// 异常任务ID（连接器自身异常时为空）
    pub failed_tasks: Vec<u32>,
    /// 失败原因（trace 首行；正常时为 `None`）
    pub reason: Option<String>,
}

/// # 归一化 Kafka Connect 地址
///
/// 去掉首尾空白与末尾 `/`。既用作请求基地址，也用于判断两个地址是否指向同一台
/// 集群（改地址前要先确认「是否真的换了集群」）。
pub fn normalize_url(url: &str) -> String {
    url.trim().trim_end_matches('/').to_string()
}

/// # 这次修改要不要主动下发
///
/// 上游、下游的修改钩子共用这一处判定。三种情况要下发：
///
/// * **配置变了** —— 健康检查只在「集群上没有这个连接器」时才部署，不下发就永远
///   不会生效；
/// * **换了位置或改了名** —— 旧位置上的那个刚被注销，新位置上还没有；
/// * **从禁用改为启用** —— 禁用时连接器已被注销，不重新部署的话，开关显示「已启用」
///   而集群上一直没有它，要等下一轮检查才补上。
///
/// 禁用状态下恒不下发：那会把刚注销的连接器又建回来，与「已禁用」矛盾。
///
/// ## 参数
/// * `enabled_after` - 这次修改之后的启用状态
/// * `config_changed` - 配置是否变化
/// * `relocated` - 集群地址或连接器名是否变化
/// * `enabling` - 是否由禁用改为启用
///
/// ## 返回值
/// * `bool` - 需要下发时为 `true`
pub fn should_deploy(
    enabled_after: bool,
    config_changed: bool,
    relocated: bool,
    enabling: bool,
) -> bool {
    enabled_after && (config_changed || relocated || enabling)
}

/// # 连接器操作错误
///
/// 用于区分 Kafka Connect 调用失败的具体原因，便于业务层映射为不同的运行状态。
#[derive(Debug)]
pub enum KafkaConnectorError {
    /// 请求失败（网络异常或返回非 2xx 状态码）
    Request(String),
    /// 连接器不存在（HTTP 404）
    NotFound,
    /// 状态响应解析失败
    Parse(String),
}

impl std::fmt::Display for KafkaConnectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KafkaConnectorError::Request(msg) => write!(f, "请求失败: {msg}"),
            KafkaConnectorError::NotFound => write!(f, "连接器不存在"),
            KafkaConnectorError::Parse(msg) => write!(f, "响应解析失败: {msg}"),
        }
    }
}

impl std::error::Error for KafkaConnectorError {}

/// # Kafka 连接器工具
pub struct KafkaConnectorUtils;

impl KafkaConnectorUtils {
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
    /// * `Err(KafkaConnectorError)` - 部署失败
    pub async fn deploy_connector(
        base_url: &str,
        connector_name: &str,
        connector_config: &str,
    ) -> Result<(), KafkaConnectorError> {
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
                KafkaConnectorError::Request(format!("部署连接器<{connector_name}>失败: {e}"))
            })?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(KafkaConnectorError::Request(format!(
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

    /// # 查询连接器状态（含任务状态与失败原因）
    ///
    /// 调用 `GET /connectors/{name}/status`，把连接器与任务的状态一起读回来。
    ///
    /// Kafka Connect 里**任务失败不会改变 `connector.state`**：任务全挂了，连接器
    /// 仍然报 `RUNNING`。只看连接器状态会把「连接器在跑、任务全挂」判成正常，
    /// 所以这里必须同时检查 `tasks`。
    ///
    /// 失败原因（[`ConnectorHealth::reason`]）取自 trace 首行：连接器自身异常时取
    /// 连接器级 trace，任务异常时取第一个失败任务的 trace 并带上任务号。写库前由
    /// [`one_line`](crate::utils::one_line) 压成一行。
    ///
    /// ## 参数
    /// * `base_url` - Kafka Connect 地址
    /// * `connector_name` - 连接器名称
    ///
    /// ## 返回值
    /// * `Ok(ConnectorHealth)` - 状态 + 异常任务ID + 失败原因首行。
    ///   任务有 `FAILED` / `UNASSIGNED` 时，`state` 返回 `Failed` 且 `failed_tasks` 非空；
    ///   连接器自身非 `RUNNING` 时以连接器状态为准，`failed_tasks` 为空
    /// * `Err(KafkaConnectorError)` - 查询失败，可通过错误类型区分具体原因
    pub async fn fetch_connector_status(
        base_url: &str,
        connector_name: &str,
    ) -> Result<ConnectorHealth, KafkaConnectorError> {
        let url = format!(
            "{}/connectors/{}/status",
            base_url.trim_end_matches('/'),
            connector_name
        );
        let resp = HTTP_CLIENT.get(&url).send().await.map_err(|e| {
            KafkaConnectorError::Request(format!("查询连接器<{connector_name}>状态失败: {e}"))
        })?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(KafkaConnectorError::NotFound);
        }
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(KafkaConnectorError::Request(format!(
                "查询连接器<{connector_name}>状态失败: {body}"
            )));
        }
        let status_resp: ConnectorStatusResponse = resp.json().await.map_err(|e| {
            KafkaConnectorError::Parse(format!("解析连接器<{connector_name}>状态响应失败: {e}"))
        })?;

        // 连接器自身不正常时以它为准（任务状态这时没有额外信息）
        let connector_state = parse_connector_state(&status_resp.connector.state);
        if connector_state != KafkaConnectorStatus::Running {
            return Ok(ConnectorHealth {
                state: connector_state,
                failed_tasks: Vec::new(),
                reason: status_resp
                    .connector
                    .trace
                    .as_deref()
                    .and_then(|trace| trace_head(trace, TRACE_HEAD_MAX)),
            });
        }

        // 连接器 RUNNING 但任务可能挂了：FAILED / UNASSIGNED 都算异常。
        // 只重启这几个任务，比重启整个连接器影响小。
        let failed_tasks: Vec<&ConnectorTaskState> = status_resp
            .tasks
            .iter()
            .filter(|task| is_task_abnormal(parse_connector_state(&task.state)))
            .collect();

        if failed_tasks.is_empty() {
            return Ok(ConnectorHealth {
                state: KafkaConnectorStatus::Running,
                failed_tasks: Vec::new(),
                reason: None,
            });
        }

        let failed_task_ids: Vec<u32> = failed_tasks.iter().map(|task| task.id).collect();
        warn!(
            "连接器<{connector_name}>自身为 RUNNING，但有 {} 个任务异常: {failed_task_ids:?}",
            failed_task_ids.len()
        );

        // 取第一个带 trace 的失败任务：它的异常才是「为什么挂了」
        let reason = failed_tasks.iter().find_map(|task| {
            task.trace
                .as_deref()
                .and_then(|trace| trace_head(trace, TRACE_HEAD_MAX))
                .map(|head| format!("任务 #{}: {head}", task.id))
        });

        Ok(ConnectorHealth {
            state: KafkaConnectorStatus::Failed,
            failed_tasks: failed_task_ids,
            reason,
        })
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
    /// * `Err(KafkaConnectorError)` - 请求失败
    pub async fn connector_exists(
        base_url: &str,
        connector_name: &str,
    ) -> Result<bool, KafkaConnectorError> {
        let url = format!(
            "{}/connectors/{}",
            base_url.trim_end_matches('/'),
            connector_name
        );
        let resp = HTTP_CLIENT.get(&url).send().await.map_err(|e| {
            KafkaConnectorError::Request(format!("检查连接器<{connector_name}>是否存在失败: {e}"))
        })?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(false);
        }
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(KafkaConnectorError::Request(format!(
                "检查连接器<{connector_name}>是否存在失败: {body}"
            )));
        }
        Ok(true)
    }

    /// # 查看连接器插件
    ///
    /// 调用 `GET /connector-plugins`，返回 Kafka Connect 已安装的全部连接器插件。
    /// 常用于在新建连接器前确认目标环境是否已安装对应的 Debezium 插件。
    ///
    /// ## 参数
    /// * `base_url` - Kafka Connect 地址
    ///
    /// ## 返回值
    /// * `Ok(Vec<KafkaConnectorPluginVo>)` - 插件列表，未安装任何插件时为空 Vec
    /// * `Err(KafkaConnectorError)` - 请求失败或响应解析失败
    pub async fn list_connector_plugins(
        base_url: &str,
    ) -> Result<Vec<KafkaConnectorPluginVo>, KafkaConnectorError> {
        let url = format!("{}/connector-plugins", base_url.trim_end_matches('/'));
        let resp = HTTP_CLIENT
            .get(&url)
            .send()
            .await
            .map_err(|e| KafkaConnectorError::Request(format!("查询连接器插件失败: {e}")))?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(KafkaConnectorError::Request(format!(
                "查询连接器插件失败: {body}"
            )));
        }
        let plugins: Vec<KafkaConnectorPluginVo> = resp
            .json()
            .await
            .map_err(|e| KafkaConnectorError::Parse(format!("解析连接器插件响应失败: {e}")))?;
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
    /// * `Ok(Vec<KafkaConnectorVo>)` - 连接器列表，无连接器时为空 Vec
    /// * `Err(KafkaConnectorError)` - 请求失败或响应解析失败
    pub async fn list_connectors(
        base_url: &str,
    ) -> Result<Vec<KafkaConnectorVo>, KafkaConnectorError> {
        let url = format!("{}/connectors", base_url.trim_end_matches('/'));
        let resp = HTTP_CLIENT
            .get(&url)
            .query(&[("expand", "info"), ("expand", "status")])
            .send()
            .await
            .map_err(|e| KafkaConnectorError::Request(format!("查询连接器列表失败: {e}")))?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(KafkaConnectorError::Request(format!(
                "查询连接器列表失败: {body}"
            )));
        }
        let value: Value = resp
            .json()
            .await
            .map_err(|e| KafkaConnectorError::Parse(format!("解析连接器列表响应失败: {e}")))?;

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
                        .map(|health| health.state)
                        .unwrap_or(KafkaConnectorStatus::Unassigned);
                    connectors.push(KafkaConnectorVo {
                        name: name.to_string(),
                        connector_class: String::new(),
                        connector_type: String::new(),
                        state,
                        worker_id: None,
                        trace: None,
                        tasks: Vec::new(),
                    });
                }
                warn!("Kafka Connect 未返回 expand 信息，连接器列表已退化为仅含名称与状态");
                connectors
            }
            _ => {
                return Err(KafkaConnectorError::Parse(
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
    /// * `Err(KafkaConnectorError)` - 连接器不存在或请求失败
    pub async fn restart_connector(
        base_url: &str,
        connector_name: &str,
        include_tasks: bool,
        only_failed: bool,
    ) -> Result<(), KafkaConnectorError> {
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
                KafkaConnectorError::Request(format!("重启连接器<{connector_name}>失败: {e}"))
            })?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(KafkaConnectorError::NotFound);
        }
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(KafkaConnectorError::Request(format!(
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
    /// * `Err(KafkaConnectorError)` - 连接器或任务不存在、请求失败
    pub async fn restart_connector_task(
        base_url: &str,
        connector_name: &str,
        task_id: u32,
    ) -> Result<(), KafkaConnectorError> {
        let url = format!(
            "{}/connectors/{}/tasks/{}/restart",
            base_url.trim_end_matches('/'),
            connector_name,
            task_id
        );
        let resp = HTTP_CLIENT.post(&url).send().await.map_err(|e| {
            KafkaConnectorError::Request(format!(
                "重启连接器<{connector_name}>的任务<{task_id}>失败: {e}"
            ))
        })?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(KafkaConnectorError::NotFound);
        }
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(KafkaConnectorError::Request(format!(
                "重启连接器<{connector_name}>的任务<{task_id}>失败: {body}"
            )));
        }
        info!("连接器<{connector_name}>的任务<{task_id}>重启请求已接受");
        Ok(())
    }

    /// # 停止连接器
    ///
    /// 调用 `PUT /connectors/{name}/stop`，让连接器停下来但**保留配置与位点**：
    /// 任务会被撤回，之后 [`resume_connector`](Self::resume_connector) 能从原处继续。
    /// 这是 KIP-875 引入的接口，Kafka 3.5 之前的集群没有（会返回 405 或 404），
    /// 那时只能用暂停（`PAUSED`）或直接删连接器。
    ///
    /// ## 参数
    /// * `base_url` - Kafka Connect 地址
    /// * `connector_name` - 连接器名称
    ///
    /// ## 返回值
    /// * `Ok(())` - 停止请求已被接受（停机是异步的）
    /// * `Err(KafkaConnectorError)` - 连接器不存在、集群版本不支持或请求失败
    pub async fn stop_connector(
        base_url: &str,
        connector_name: &str,
    ) -> Result<(), KafkaConnectorError> {
        Self::control(
            reqwest::Method::PUT,
            base_url,
            &format!("connectors/{connector_name}/stop"),
            "停止",
            connector_name,
        )
        .await?;
        info!("连接器<{connector_name}>停止请求已接受");
        Ok(())
    }

    /// # 恢复连接器
    ///
    /// 调用 `PUT /connectors/{name}/resume`。既能恢复 `PAUSED`，也能让被
    /// [`stop_connector`](Self::stop_connector) 停下的连接器重新拉起任务；对本来就在
    /// 运行的连接器是空操作。
    ///
    /// ## 参数
    /// * `base_url` - Kafka Connect 地址
    /// * `connector_name` - 连接器名称
    ///
    /// ## 返回值
    /// * `Ok(())` - 恢复请求已被接受
    /// * `Err(KafkaConnectorError)` - 连接器不存在或请求失败
    pub async fn resume_connector(
        base_url: &str,
        connector_name: &str,
    ) -> Result<(), KafkaConnectorError> {
        Self::control(
            reqwest::Method::PUT,
            base_url,
            &format!("connectors/{connector_name}/resume"),
            "恢复",
            connector_name,
        )
        .await?;
        info!("连接器<{connector_name}>恢复请求已接受");
        Ok(())
    }

    /// # 清空连接器位点
    ///
    /// 调用 `DELETE /connectors/{name}/offsets`，把连接器记在 Kafka 里的位点删掉。
    /// 用处是「让它当作从没跑过」：Debezium 的 `snapshot.mode=initial` 只在没有位点
    /// 时才做初始快照，所以清位点 + 恢复运行等于重做一次全量。
    ///
    /// **要求连接器先处于停止状态**，否则 Kafka Connect 会返回 400
    /// （`Connector ... is not stopped`）。本方法不去替你停：停与不停是运维的决定，
    /// 调用方（服务层）先发 `/stop` 再来这里。
    ///
    /// ## 参数
    /// * `base_url` - Kafka Connect 地址
    /// * `connector_name` - 连接器名称
    ///
    /// ## 返回值
    /// * `Ok(())` - 位点已删除
    /// * `Err(KafkaConnectorError)` - 连接器不存在、未停止或请求失败
    pub async fn reset_connector_offsets(
        base_url: &str,
        connector_name: &str,
    ) -> Result<(), KafkaConnectorError> {
        Self::control(
            reqwest::Method::DELETE,
            base_url,
            &format!("connectors/{connector_name}/offsets"),
            "清空位点",
            connector_name,
        )
        .await?;
        info!("连接器<{connector_name}>的位点已清空");
        Ok(())
    }

    /// # 发一条没有请求体的控制指令
    ///
    /// stop / resume / 清空位点三者形状一致（一次 HTTP、成功空响应、404 表示连接器
    /// 不存在），差别只有方法与路径，抽一处免得三份几乎相同的错误处理各自漂移。
    ///
    /// ## 参数
    /// * `method` - HTTP 方法
    /// * `base_url` - Kafka Connect 地址
    /// * `path` - 相对路径（不含前导 `/`）
    /// * `action` - 动作名，用于拼错误文案
    /// * `connector_name` - 连接器名称（只用于文案）
    ///
    /// ## 返回值
    /// * `Ok(())` - 指令已被接受
    /// * `Err(KafkaConnectorError)` - 连接器不存在或请求失败
    async fn control(
        method: reqwest::Method,
        base_url: &str,
        path: &str,
        action: &str,
        connector_name: &str,
    ) -> Result<(), KafkaConnectorError> {
        let url = format!("{}/{}", base_url.trim_end_matches('/'), path);
        let is_get = method == reqwest::Method::GET;
        let resp = HTTP_CLIENT
            .request(method, &url)
            .send()
            .await
            .map_err(|e| {
                KafkaConnectorError::Request(format!("{action}连接器<{connector_name}>失败: {e}"))
            })?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(KafkaConnectorError::NotFound);
        }
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            // 405 / 501 几乎只出现在「集群版本不支持这个指令」上，直接把这点说出来
            if !is_get
                && (status == reqwest::StatusCode::METHOD_NOT_ALLOWED
                    || status == reqwest::StatusCode::NOT_IMPLEMENTED)
            {
                return Err(KafkaConnectorError::Request(format!(
                    "{action}连接器<{connector_name}>失败: 目标 Kafka Connect 不支持该操作\
                     （HTTP {status}），停止/恢复需要 Kafka 3.5 及以上：{body}"
                )));
            }
            return Err(KafkaConnectorError::Request(format!(
                "{action}连接器<{connector_name}>失败: {body}"
            )));
        }
        Ok(())
    }
}

/// # Kafka Connect 连接器状态响应
#[derive(Debug, Deserialize)]
struct ConnectorStatusResponse {
    /// 连接器状态
    connector: ConnectorState,
    /// 任务状态列表
    #[serde(default)]
    tasks: Vec<ConnectorTaskState>,
}

/// # Kafka Connect 连接器状态
#[derive(Debug, Deserialize)]
struct ConnectorState {
    /// 状态字符串，如 RUNNING / FAILED / PAUSED / UNASSIGNED / DESTROYED
    #[serde(default)]
    state: String,
    /// 执行该连接器的 worker
    #[serde(default)]
    worker_id: Option<String>,
    /// 异常堆栈（连接器自身失败时才有）
    #[serde(default)]
    trace: Option<String>,
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

/// # 将 expand 响应中的单个连接器转换为 [`KafkaConnectorVo`]
fn parse_expanded_connector(
    name: &str,
    value: Value,
) -> Result<KafkaConnectorVo, KafkaConnectorError> {
    let expanded: ExpandedConnector = serde_json::from_value(value)
        .map_err(|e| KafkaConnectorError::Parse(format!("解析连接器<{name}>信息失败: {e}")))?;

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
    let connector_state = expanded
        .status
        .as_ref()
        .and_then(|status| status.connector.as_ref())
        .map(|connector| parse_connector_state(&connector.state))
        .unwrap_or(KafkaConnectorStatus::Unassigned);
    // 连接器级的 worker 与 trace：状态失败时，页面上鼠标悬停就能看到原因，
    // 不必再发一次 `/status` 请求
    let worker_id = expanded
        .status
        .as_ref()
        .and_then(|status| status.connector.as_ref())
        .and_then(|connector| connector.worker_id.clone());
    let trace = expanded
        .status
        .as_ref()
        .and_then(|status| status.connector.as_ref())
        .and_then(|connector| connector.trace.clone());
    let tasks: Vec<KafkaConnectorTaskVo> = expanded
        .status
        .map(|status| {
            status
                .tasks
                .into_iter()
                .map(|task| KafkaConnectorTaskVo {
                    id: task.id,
                    state: parse_connector_state(&task.state),
                    worker_id: task.worker_id,
                    trace: task.trace,
                })
                .collect()
        })
        .unwrap_or_default();

    // 与检查任务用同一套判定：任务挂了连接器仍报 RUNNING，
    // 列表里也不能显示成「运行中」，否则同一时刻两处说法不一致。
    let state = if connector_state == KafkaConnectorStatus::Running
        && tasks.iter().any(|task| is_task_abnormal(task.state))
    {
        KafkaConnectorStatus::Failed
    } else {
        connector_state
    };

    Ok(KafkaConnectorVo {
        name: name.to_string(),
        connector_class,
        connector_type,
        state,
        worker_id,
        trace,
        tasks,
    })
}

/// # 将连接器配置字符串解析为 JSON
fn parse_config(connector_config: &str) -> Result<Value, KafkaConnectorError> {
    serde_json::from_str(connector_config)
        .map_err(|e| KafkaConnectorError::Parse(format!("连接器配置不是合法的 JSON: {e}")))
}

/// # 任务状态是否算异常
///
/// 只有 `FAILED` / `UNASSIGNED` 需要处理（重启任务）；`PAUSED` 通常是人工操作，
/// `RUNNING` 正常。
///
/// 状态查询与连接器列表共用这一处判定，保证两边对「健康」的说法一致。
fn is_task_abnormal(state: KafkaConnectorStatus) -> bool {
    matches!(
        state,
        KafkaConnectorStatus::Failed | KafkaConnectorStatus::Unassigned
    )
}

/// # 将 Kafka Connect 状态字符串映射为 [`KafkaConnectorStatus`]
fn parse_connector_state(state: &str) -> KafkaConnectorStatus {
    match state.to_uppercase().as_str() {
        "RUNNING" => KafkaConnectorStatus::Running,
        "FAILED" => KafkaConnectorStatus::Failed,
        "PAUSED" => KafkaConnectorStatus::Paused,
        "STOPPED" => KafkaConnectorStatus::Stopped,
        "DESTROYED" => KafkaConnectorStatus::Destroyed,
        _ => KafkaConnectorStatus::Unassigned,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 换集群/改名、改配置都要下发，否则新位置上什么也没有、旧配置永远不生效
    #[test]
    fn deploy_on_relocate_or_config_change() {
        assert!(should_deploy(true, false, true, false));
        assert!(should_deploy(true, true, false, false));
        assert!(should_deploy(true, true, true, false));
    }

    /// 什么都不变就别下发：PUT 一次会重建 task，白折腾一次数据流
    #[test]
    fn skip_when_nothing_changed() {
        assert!(!should_deploy(true, false, false, false));
    }

    /// 重新启用必须下发 —— 禁用时连接器已被注销，不部署就一直是「已启用但集群上没有」
    #[test]
    fn deploy_when_enabling() {
        assert!(should_deploy(true, false, false, true));
    }

    /// 禁用状态下任何理由都不下发，否则刚注销的连接器又被建回来
    #[test]
    fn never_deploy_when_disabled() {
        assert!(!should_deploy(false, true, true, false));
        assert!(!should_deploy(false, true, true, true));
    }

    /// 地址末尾斜杠与空白不影响「是不是同一台集群」的判断
    #[test]
    fn normalize_trims_slash_and_space() {
        assert_eq!(
            normalize_url(" http://10.0.0.1:8083/ "),
            "http://10.0.0.1:8083"
        );
    }
}
