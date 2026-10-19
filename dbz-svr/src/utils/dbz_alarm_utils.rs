//! # 告警工具
//!
//! 通过 alarm-api-client 向 alarm-svr 上报 Debezium 连接器异常事件。
//!
//! 上游、下游连接器各自独立告警：`business_id` 分别是 `dbz_source_connector.id`
//! 与 `dbz_sink_connector.id`；文案里带上连接器名称、所属 Kafka Connect 名称与
//! 地址，便于定位。Kafka Connect 本身不单独作为告警主体 —— 一台集群不可达时，
//! 由挂在它下面的连接器告警表达，避免同一个根因换一个 business_id 再报一遍。
//!
//! 需要先在 alarm-svr 中配置好事件来源、事件策略与告警，并将对应的
//! `event-code` 与 `alert-id` 写入 `[alarm]` 配置段，否则将跳过告警上报并打印告警日志。

use crate::config::get_app_config;
use crate::dic::ConnectorRunStatus;
use crate::vo::{DbzKafkaConnectVo, DbzSinkConnectorVo, DbzSourceConnectorVo};
use alarm_api::dto::AlarmEventLogAddDto;
use alarm_api_client::AlarmEventLogApiClient;
use robotech::api::U64;
use robotech::api_client::ApiClientConfig;
use robotech::micro_svc::FeignApiClient;
use tracing::{info, warn};
use wheel_rs::time_utils::now_ms;

/// # 告警工具
pub struct DbzAlarmUtils;

impl DbzAlarmUtils {
    /// # 上报上游连接器异常告警
    ///
    /// 当上游连接器的运行状态不为「运行中」时，向 alarm-svr 上报一条告警事件日志。
    /// 若未配置 `[alarm] event-code` 或 `[alarm] alert-id`，则跳过上报。
    ///
    /// ## 参数
    /// * `source` - 发生异常的上游连接器
    /// * `kafka_connect` - 运行该连接器的 Kafka Connect
    /// * `run_status` - 该连接器当前的运行状态
    ///
    /// ## 返回值
    /// * `Ok(())` - 上报成功或已跳过
    /// * `Err(anyhow::Error)` - 上报失败
    pub async fn send_source_connector_alarm(
        source: &DbzSourceConnectorVo,
        kafka_connect: &DbzKafkaConnectVo,
        run_status: ConnectorRunStatus,
    ) -> Result<(), anyhow::Error> {
        let title = format!("Debezium 上游连接器<{}>异常", source.name);
        let content = format!(
            "上游连接器<{}>运行状态异常: {} ({}), 连接器<{}>, Kafka Connect<{}>: {}",
            source.name,
            run_status,
            run_status.value(),
            source.connector_name,
            kafka_connect.name,
            kafka_connect.url,
        );
        Self::send(title, content, source.id).await
    }

    /// # 上报下游连接器异常告警
    ///
    /// 当下游连接器的运行状态不为「运行中」时，向 alarm-svr 上报一条告警事件日志。
    /// 若未配置 `[alarm] event-code` 或 `[alarm] alert-id`，则跳过上报。
    ///
    /// ## 参数
    /// * `sink` - 发生异常的下游连接器
    /// * `source_name` - 该下游连接器所属上游连接器的名称（仅用于文案定位）
    /// * `kafka_connect` - 运行该连接器的 Kafka Connect
    /// * `run_status` - 该连接器当前的运行状态
    ///
    /// ## 返回值
    /// * `Ok(())` - 上报成功或已跳过
    /// * `Err(anyhow::Error)` - 上报失败
    pub async fn send_sink_connector_alarm(
        sink: &DbzSinkConnectorVo,
        source_name: &str,
        kafka_connect: &DbzKafkaConnectVo,
        run_status: ConnectorRunStatus,
    ) -> Result<(), anyhow::Error> {
        let title = format!("Debezium 下游连接器<{}>异常", sink.name);
        let content = format!(
            "下游连接器<{}>运行状态异常: {} ({}), 连接器<{}>, 上游连接器<{}>, Kafka Connect<{}>: {}",
            sink.name,
            run_status,
            run_status.value(),
            sink.connector_name,
            source_name,
            kafka_connect.name,
            kafka_connect.url,
        );
        Self::send(title, content, sink.id).await
    }

    /// # 上报一条连接器异常告警
    ///
    /// ## 参数
    /// * `title` - 告警标题
    /// * `content` - 告警内容
    /// * `business_id` - 业务ID（连接器记录的ID）
    async fn send(title: String, content: String, business_id: U64) -> Result<(), anyhow::Error> {
        let app_config = get_app_config()?;
        let (Some(event_code), Some(alert_id)) = (
            app_config.alarm.event_code.clone(),
            app_config.alarm.alert_id,
        ) else {
            warn!("未配置 [alarm] event-code / alert-id，跳过告警上报");
            return Ok(());
        };

        let api_config = ApiClientConfig::Simple {
            base_url: app_config.alarm.base_url.clone(),
            auth: None,
        };
        let alarm_client = AlarmEventLogApiClient::new(FeignApiClient::new(api_config).await);

        let add_dto = AlarmEventLogAddDto::builder()
            .event_code(event_code)
            .business_id(business_id)
            .alert_id(U64(alert_id))
            .trigger_ms(U64(now_ms()))
            .title(title)
            .content(content)
            ._current_user_id(U64(1))
            .build();

        let ro = alarm_client.add(&add_dto).await?;
        info!("连接器异常告警上报成功: {}", ro.msg);
        Ok(())
    }
}
