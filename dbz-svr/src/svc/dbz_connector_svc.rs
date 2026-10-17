//! # 连接器服务
//!
//! 面向 Kafka Connect 的运维能力：查看连接器插件、查看所有连接器、
//! 重启连接器（可连同其任务）与重启单个任务。
//!
//! 与实体服务不同，本服务没有对应的数据库实体，因此不使用 `#[svc]` 宏，
//! 而是手写 `impl` 块；目标 Kafka Connect 通过 DTO 中的
//! `kafkaConnectUrl` 或 `instanceId` 定位（见 `DbzConnectorQueryDto` 的说明）。

use crate::dto::{DbzConnectorQueryDto, DbzConnectorRestartDto, DbzConnectorTaskRestartDto};
use crate::svc::DbzInstanceSvc;
use crate::utils::{ConnectorError, DbzConnectorUtils};
use crate::vo::{ConnectorPluginVo, ConnectorVo};
use anyhow::anyhow;
use robotech::api::{Ro, U64};
use robotech::macros::log_call;
use sea_orm::DatabaseConnection;
use tracing::info;

/// # 连接器服务
pub struct DbzConnectorSvc;

impl DbzConnectorSvc {
    /// # 查看连接器插件
    ///
    /// 查询目标 Kafka Connect 已安装的连接器插件，用于确认 Debezium 插件是否就绪。
    ///
    /// ## 参数
    /// * `dto` - 目标 Kafka Connect 的定位参数
    ///
    /// ## 返回值
    /// * `Ok(Ro<Vec<ConnectorPluginVo>>)` - 插件列表
    /// * `Err(anyhow::Error)` - 定位失败、Kafka Connect 不可用或响应解析失败
    #[log_call]
    pub async fn list_connector_plugins(
        dto: DbzConnectorQueryDto,
    ) -> Result<Ro<Vec<ConnectorPluginVo>>, anyhow::Error> {
        let base_url =
            Self::resolve_base_url(dto.instance_id, dto.kafka_connect_url.as_deref()).await?;
        let plugins = DbzConnectorUtils::list_connector_plugins(&base_url).await?;
        Ok(
            Ro::success(format!("查询连接器插件成功，共 {} 个", plugins.len()))
                .extra(Some(plugins)),
        )
    }

    /// # 查看所有连接器
    ///
    /// 查询目标 Kafka Connect 上的全部连接器，包含连接器与各任务的运行状态，
    /// 便于发现未被 dbz 实例纳管的连接器以及定位失败的任务。
    ///
    /// ## 参数
    /// * `dto` - 目标 Kafka Connect 的定位参数
    ///
    /// ## 返回值
    /// * `Ok(Ro<Vec<ConnectorVo>>)` - 连接器列表
    /// * `Err(anyhow::Error)` - 定位失败、Kafka Connect 不可用或响应解析失败
    #[log_call]
    pub async fn list_connectors(
        dto: DbzConnectorQueryDto,
    ) -> Result<Ro<Vec<ConnectorVo>>, anyhow::Error> {
        let base_url =
            Self::resolve_base_url(dto.instance_id, dto.kafka_connect_url.as_deref()).await?;
        let connectors = DbzConnectorUtils::list_connectors(&base_url).await?;
        Ok(
            Ro::success(format!("查询连接器成功，共 {} 个", connectors.len()))
                .extra(Some(connectors)),
        )
    }

    /// # 重启连接器
    ///
    /// 重启指定连接器，默认连同其任务一起重启。Kafka Connect 的重启是异步的，
    /// 接口返回成功仅代表重启请求已被接受，实际状态请稍后重新查询。
    ///
    /// ## 参数
    /// * `dto` - 目标 Kafka Connect 的定位参数 + 连接器名称 + 重启选项
    ///
    /// ## 返回值
    /// * `Ok(Ro<()>)` - 重启请求已提交
    /// * `Err(anyhow::Error)` - 连接器不存在、定位失败或请求失败
    #[log_call]
    pub async fn restart_connector(dto: DbzConnectorRestartDto) -> Result<Ro<()>, anyhow::Error> {
        let connector_name = Self::check_connector_name(&dto.connector_name)?;
        let base_url =
            Self::resolve_base_url(dto.instance_id, dto.kafka_connect_url.as_deref()).await?;

        DbzConnectorUtils::restart_connector(
            &base_url,
            connector_name,
            dto.include_tasks,
            dto.only_failed,
        )
        .await
        .map_err(|e| match e {
            ConnectorError::NotFound => anyhow!("连接器<{connector_name}>不存在"),
            other => other.into(),
        })?;

        info!(
            "连接器<{connector_name}>重启请求已提交(includeTasks={}, onlyFailed={})",
            dto.include_tasks, dto.only_failed
        );
        Ok(Ro::success(format!(
            "连接器<{connector_name}>重启请求已提交，Kafka Connect 将异步重启"
        )))
    }

    /// # 重启连接器的单个任务
    ///
    /// 仅重启指定任务，适用于连接器本身正常、个别任务失败（FAILED）的场景。
    /// 任务ID可从「查看所有连接器」接口返回的 `tasks` 中获取。
    ///
    /// ## 参数
    /// * `dto` - 目标 Kafka Connect 的定位参数 + 连接器名称 + 任务ID
    ///
    /// ## 返回值
    /// * `Ok(Ro<()>)` - 重启请求已提交
    /// * `Err(anyhow::Error)` - 连接器或任务不存在、定位失败或请求失败
    #[log_call]
    pub async fn restart_connector_task(
        dto: DbzConnectorTaskRestartDto,
    ) -> Result<Ro<()>, anyhow::Error> {
        let connector_name = Self::check_connector_name(&dto.connector_name)?;
        let base_url =
            Self::resolve_base_url(dto.instance_id, dto.kafka_connect_url.as_deref()).await?;

        DbzConnectorUtils::restart_connector_task(&base_url, connector_name, dto.task_id)
            .await
            .map_err(|e| match e {
                ConnectorError::NotFound => {
                    anyhow!("连接器<{connector_name}>或其任务<{}>不存在", dto.task_id)
                }
                other => other.into(),
            })?;

        info!(
            "连接器<{connector_name}>的任务<{}>重启请求已提交",
            dto.task_id
        );
        Ok(Ro::success(format!(
            "连接器<{connector_name}>的任务<{}>重启请求已提交",
            dto.task_id
        )))
    }

    /// # 定位目标 Kafka Connect 地址
    ///
    /// 优先使用显式传入的 `kafkaConnectUrl`；未提供时按 `instanceId` 查询实例，
    /// 取其配置的 Kafka Connect 地址。
    ///
    /// ## 参数
    /// * `instance_id` - 实例ID（可选）
    /// * `kafka_connect_url` - Kafka Connect 地址（可选，优先）
    ///
    /// ## 返回值
    /// * `Ok(String)` - 去掉末尾 `/` 的 Kafka Connect 地址
    /// * `Err(anyhow::Error)` - 两者都未提供、实例不存在或实例未配置地址
    async fn resolve_base_url(
        instance_id: Option<U64>,
        kafka_connect_url: Option<&str>,
    ) -> Result<String, anyhow::Error> {
        if let Some(url) = kafka_connect_url
            .map(str::trim)
            .filter(|url| !url.is_empty())
        {
            return Ok(url.trim_end_matches('/').to_string());
        }

        let Some(id) = instance_id else {
            return Err(anyhow!("必须提供 kafkaConnectUrl 或 instanceId 之一"));
        };

        let ro = DbzInstanceSvc::get_by_id::<DatabaseConnection>(id, None)
            .await
            .map_err(|e| anyhow!("查询实例<{}>失败: {e}", id.value()))?;
        let instance = ro
            .extra
            .ok_or_else(|| anyhow!("实例<{}>不存在", id.value()))?;

        let url = instance.kafka_connect_url.trim().trim_end_matches('/');
        if url.is_empty() {
            return Err(anyhow!("实例<{}>未配置 Kafka Connect 地址", instance.name));
        }
        Ok(url.to_string())
    }

    /// # 校验连接器名称
    ///
    /// ## 参数
    /// * `connector_name` - 待校验的连接器名称
    ///
    /// ## 返回值
    /// * `Ok(&str)` - 去除首尾空白后的连接器名称
    /// * `Err(anyhow::Error)` - 名称为空
    fn check_connector_name(connector_name: &str) -> Result<&str, anyhow::Error> {
        let connector_name = connector_name.trim();
        if connector_name.is_empty() {
            return Err(anyhow!("连接器名称不能为空"));
        }
        Ok(connector_name)
    }
}
