//! # Kafka 连接器服务
//!
//! 面向 Kafka Connect 的运维能力：查看连接器插件、查看所有连接器、
//! 重启连接器（可连同其任务）、重启单个任务，以及删除连接器。
//!
//! 与实体服务（`dbz_*`）不同，本服务没有对应的数据库实体，因此不使用 `#[svc]`
//! 宏，而是手写 `impl` 块：它只把请求转发到集群、再把响应转成 VO，不读写本库。
//! 目标 Kafka Connect 通过 DTO 中的 `kafkaConnectUrl` 或 `kafkaConnectId` 定位
//! （见 `KafkaConnectorQueryDto` 的说明）。

use crate::dto::{
    KafkaConnectorControlDto, KafkaConnectorDeleteDto, KafkaConnectorQueryDto,
    KafkaConnectorResetOffsetDto, KafkaConnectorRestartDto, KafkaConnectorTaskRestartDto,
};
use crate::svc::DbzKafkaConnectSvc;
use crate::utils::{KafkaConnectorError, KafkaConnectorUtils, normalize_url};
use crate::vo::{KafkaConnectorPluginVo, KafkaConnectorVo};
use anyhow::anyhow;
use robotech::api::{Ro, U64};
use robotech::macros::log_call;
use tracing::info;

/// # Kafka 连接器服务
pub struct KafkaConnectorSvc;

impl KafkaConnectorSvc {
    /// # 查看连接器插件
    ///
    /// 查询目标 Kafka Connect 已安装的连接器插件，用于确认 Debezium 插件是否就绪。
    ///
    /// ## 参数
    /// * `dto` - 目标 Kafka Connect 的定位参数
    ///
    /// ## 返回值
    /// * `Ok(Ro<Vec<KafkaConnectorPluginVo>>)` - 插件列表
    /// * `Err(anyhow::Error)` - 定位失败、Kafka Connect 不可用或响应解析失败
    #[log_call]
    pub async fn list_connector_plugins(
        dto: KafkaConnectorQueryDto,
    ) -> Result<Ro<Vec<KafkaConnectorPluginVo>>, anyhow::Error> {
        let base_url =
            Self::resolve_base_url(dto.kafka_connect_id, dto.kafka_connect_url.as_deref()).await?;
        let plugins = KafkaConnectorUtils::list_connector_plugins(&base_url).await?;
        Ok(
            Ro::success(format!("查询连接器插件成功，共 {} 个", plugins.len()))
                .extra(Some(plugins)),
        )
    }

    /// # 查看所有连接器
    ///
    /// 查询目标 Kafka Connect 上的全部连接器，包含连接器与各任务的运行状态，
    /// 便于发现未被 dbz 纳管的连接器以及定位失败的任务。
    ///
    /// ## 参数
    /// * `dto` - 目标 Kafka Connect 的定位参数
    ///
    /// ## 返回值
    /// * `Ok(Ro<Vec<KafkaConnectorVo>>)` - 连接器列表
    /// * `Err(anyhow::Error)` - 定位失败、Kafka Connect 不可用或响应解析失败
    #[log_call]
    pub async fn list_connectors(
        dto: KafkaConnectorQueryDto,
    ) -> Result<Ro<Vec<KafkaConnectorVo>>, anyhow::Error> {
        let base_url =
            Self::resolve_base_url(dto.kafka_connect_id, dto.kafka_connect_url.as_deref()).await?;
        let connectors = KafkaConnectorUtils::list_connectors(&base_url).await?;
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
    pub async fn restart_connector(dto: KafkaConnectorRestartDto) -> Result<Ro<()>, anyhow::Error> {
        let connector_name = Self::check_connector_name(&dto.connector_name)?;
        let base_url =
            Self::resolve_base_url(dto.kafka_connect_id, dto.kafka_connect_url.as_deref()).await?;

        KafkaConnectorUtils::restart_connector(
            &base_url,
            connector_name,
            dto.include_tasks,
            dto.only_failed,
        )
        .await
        .map_err(|e| match e {
            KafkaConnectorError::NotFound => anyhow!("连接器<{connector_name}>不存在"),
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
        dto: KafkaConnectorTaskRestartDto,
    ) -> Result<Ro<()>, anyhow::Error> {
        let connector_name = Self::check_connector_name(&dto.connector_name)?;
        let base_url =
            Self::resolve_base_url(dto.kafka_connect_id, dto.kafka_connect_url.as_deref()).await?;

        KafkaConnectorUtils::restart_connector_task(&base_url, connector_name, dto.task_id)
            .await
            .map_err(|e| match e {
                KafkaConnectorError::NotFound => {
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

    /// # 删除连接器
    ///
    /// 把连接器从目标集群上注销（`DELETE /connectors/{name}`）。只动集群、不动库：
    /// 删掉的连接器若仍被 dbz 纳管，下一轮健康检查会按记录重新部署；要彻底停掉
    /// 请禁用对应记录，那条路会先注销再改库。
    ///
    /// 连接器本就不存在（HTTP 404）视为删除成功 —— 目的是「让它不在集群上」。
    ///
    /// ## 参数
    /// * `dto` - 目标 Kafka Connect 的定位参数 + 连接器名称
    ///
    /// ## 返回值
    /// * `Ok(Ro<()>)` - 连接器已从集群上删除
    /// * `Err(anyhow::Error)` - 名称为空、定位失败或请求失败
    #[log_call]
    pub async fn delete_connector(dto: KafkaConnectorDeleteDto) -> Result<Ro<()>, anyhow::Error> {
        let connector_name = Self::check_connector_name(&dto.connector_name)?;
        let base_url =
            Self::resolve_base_url(dto.kafka_connect_id, dto.kafka_connect_url.as_deref()).await?;

        KafkaConnectorUtils::delete_connector(&base_url, connector_name).await?;

        info!("连接器<{connector_name}>已从 <{base_url}> 删除");
        Ok(Ro::success(format!(
            "连接器<{connector_name}>已从集群上删除"
        )))
    }

    /// # 停止连接器
    ///
    /// 发 `PUT /connectors/{name}/stop`：连接器停下来、任务被撤回，但**配置与位点
    /// 都保留**，恢复后从原处继续。要重做全量用
    /// [`reset_connector_offsets`](Self::reset_connector_offsets)。
    ///
    /// 停下来的连接器在健康检查里的状态是「已停止」，会被记为异常并告警（
    /// 这正是运维中断数据同步应该看到的样子），但**不会被自动重启** —— 自动修复
    /// 只处理 `FAILED`，人工动作不能被机器覆盖。
    ///
    /// ## 参数
    /// * `dto` - 目标 Kafka Connect 的定位参数 + 连接器名称
    ///
    /// ## 返回值
    /// * `Ok(Ro<()>)` - 停止请求已被接受（停机是异步的）
    /// * `Err(anyhow::Error)` - 名称为空、定位失败、连接器不存在或集群不支持该指令
    #[log_call]
    pub async fn stop_connector(dto: KafkaConnectorControlDto) -> Result<Ro<()>, anyhow::Error> {
        let connector_name = Self::check_connector_name(&dto.connector_name)?;
        let base_url =
            Self::resolve_base_url(dto.kafka_connect_id, dto.kafka_connect_url.as_deref()).await?;

        KafkaConnectorUtils::stop_connector(&base_url, connector_name)
            .await
            .map_err(|e| match e {
                KafkaConnectorError::NotFound => anyhow!("连接器<{connector_name}>不存在"),
                other => other.into(),
            })?;

        info!("连接器<{connector_name}>停止请求已提交");
        Ok(Ro::success(format!(
            "连接器<{connector_name}>停止请求已提交：配置与位点保留，下一次检查会把它记为\
             「已停止」并告警（不会自动重启）；恢复请点「恢复运行」"
        )))
    }

    /// # 恢复连接器
    ///
    /// 发 `PUT /connectors/{name}/resume`，既能恢复 `PAUSED`，也能让被
    /// [`stop_connector`](Self::stop_connector) 停下的连接器重新拉起任务。
    ///
    /// ## 参数
    /// * `dto` - 目标 Kafka Connect 的定位参数 + 连接器名称
    ///
    /// ## 返回值
    /// * `Ok(Ro<()>)` - 恢复请求已被接受
    /// * `Err(anyhow::Error)` - 名称为空、定位失败、连接器不存在或请求失败
    #[log_call]
    pub async fn resume_connector(dto: KafkaConnectorControlDto) -> Result<Ro<()>, anyhow::Error> {
        let connector_name = Self::check_connector_name(&dto.connector_name)?;
        let base_url =
            Self::resolve_base_url(dto.kafka_connect_id, dto.kafka_connect_url.as_deref()).await?;

        KafkaConnectorUtils::resume_connector(&base_url, connector_name)
            .await
            .map_err(|e| match e {
                KafkaConnectorError::NotFound => anyhow!("连接器<{connector_name}>不存在"),
                other => other.into(),
            })?;

        info!("连接器<{connector_name}>恢复请求已提交");
        Ok(Ro::success(format!(
            "连接器<{connector_name}>恢复请求已提交，Kafka Connect 将异步拉起任务"
        )))
    }

    /// # 重置连接器位点
    ///
    /// 三步：先 `stop`（Kafka Connect 要求连接器处于停止状态才允许清位点）→
    /// `DELETE /connectors/{name}/offsets` → 视 `resume` 决定要不要立刻拉起来。
    ///
    /// 位点清空后连接器就是「从没跑过」：Debezium 的 `snapshot.mode=initial` 会做
    /// 一次全量快照，等于把这张表重新同步一遍。这一步不可撤销 —— 位点删了就没有了，
    /// 想回到原来的位置只能靠消费端的 offset 记录。
    ///
    /// ## 参数
    /// * `dto` - 目标 Kafka Connect 的定位参数 + 连接器名称 + 是否立即恢复
    ///
    /// ## 返回值
    /// * `Ok(Ro<()>)` - 位点已清空（含是否已恢复）
    /// * `Err(anyhow::Error)` - 名称为空、定位失败、连接器不存在、停止失败或清位点失败
    #[log_call]
    pub async fn reset_connector_offsets(
        dto: KafkaConnectorResetOffsetDto,
    ) -> Result<Ro<()>, anyhow::Error> {
        let connector_name = Self::check_connector_name(&dto.connector_name)?;
        let base_url =
            Self::resolve_base_url(dto.kafka_connect_id, dto.kafka_connect_url.as_deref()).await?;

        // 已经停止的连接器再停一次是空操作；正在跑的先停，否则清位点会被 400 拒掉
        KafkaConnectorUtils::stop_connector(&base_url, connector_name)
            .await
            .map_err(|e| match e {
                KafkaConnectorError::NotFound => anyhow!("连接器<{connector_name}>不存在"),
                other => anyhow!("清空位点前先停止连接器失败：{other}"),
            })?;

        KafkaConnectorUtils::reset_connector_offsets(&base_url, connector_name)
            .await
            .map_err(|e| match e {
                KafkaConnectorError::NotFound => anyhow!("连接器<{connector_name}>不存在"),
                other => other.into(),
            })?;

        info!(
            "连接器<{connector_name}>的位点已清空(resume={})",
            dto.resume
        );

        if !dto.resume {
            return Ok(Ro::success(format!(
                "连接器<{connector_name}>的位点已清空，连接器仍处于停止状态：点「恢复运行」\
                 后才会重新开始，`snapshot.mode=initial` 时首轮是整表快照"
            )));
        }

        KafkaConnectorUtils::resume_connector(&base_url, connector_name)
            .await
            .map_err(|e| anyhow!("位点已清空，但恢复连接器<{connector_name}>失败：{e}"))?;
        info!("连接器<{connector_name}>位点已清空并恢复运行");
        Ok(Ro::success(format!(
            "连接器<{connector_name}>的位点已清空并已恢复运行：连接器没有位点，\
             `snapshot.mode=initial` 时开始整表快照，快照数据以 op=r 追加到原 topic，\
             下游需按主键幂等处理"
        )))
    }

    /// # 定位目标 Kafka Connect 地址
    ///
    /// 优先使用显式传入的 `kafkaConnectUrl`；未提供时按 `kafkaConnectId` 取
    /// `dbz_kafka_connect` 记录上的地址。
    ///
    /// ## 参数
    /// * `kafka_connect_id` - Kafka Connect ID（可选）
    /// * `kafka_connect_url` - Kafka Connect 地址（可选，优先）
    ///
    /// ## 返回值
    /// * `Ok(String)` - 去掉末尾 `/` 的 Kafka Connect 地址
    /// * `Err(anyhow::Error)` - 两者都未提供、集群记录不存在或未配置地址
    async fn resolve_base_url(
        kafka_connect_id: Option<U64>,
        kafka_connect_url: Option<&str>,
    ) -> Result<String, anyhow::Error> {
        if let Some(url) = kafka_connect_url
            .map(str::trim)
            .filter(|url| !url.is_empty())
        {
            return Ok(normalize_url(url));
        }

        let Some(id) = kafka_connect_id else {
            return Err(anyhow!("必须提供 kafkaConnectUrl 或 kafkaConnectId 之一"));
        };

        DbzKafkaConnectSvc::get_url(id).await
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
