//! # 告警工具
//!
//! 通过 alarm-api-client 向 alarm-svr 上报 Debezium 连接器异常事件。
//!
//! 需要先在 alarm-svr 中配置好事件来源、事件策略与告警，并将对应的
//! `event-code` 与 `alert-id` 写入 `[alarm]` 配置段，否则将跳过告警上报并打印告警日志。

use crate::config::get_app_config;
use crate::dic::InstanceRunStatus;
use crate::vo::DbzInstanceVo;
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
    /// # 上报连接器异常告警
    ///
    /// 当实例的运行状态不为「运行中」时，向 alarm-svr 上报一条告警事件日志。
    /// 若未配置 `[alarm] event-code` 或 `[alarm] alert-id`，则跳过上报。
    ///
    /// ## 参数
    /// * `instance` - 发生异常的实例
    /// * `run_status` - 实例当前的运行状态
    ///
    /// ## 返回值
    /// * `Ok(())` - 上报成功或已跳过
    /// * `Err(anyhow::Error)` - 上报失败
    pub async fn send_connector_alarm(
        instance: &DbzInstanceVo,
        run_status: InstanceRunStatus,
    ) -> Result<(), anyhow::Error> {
        let app_config = get_app_config()?;
        let (Some(event_code), Some(alert_id)) =
            (app_config.alarm.event_code.clone(), app_config.alarm.alert_id)
        else {
            warn!("未配置 [alarm] event-code / alert-id，跳过告警上报");
            return Ok(());
        };

        let api_config = ApiClientConfig::Simple {
            base_url: app_config.alarm.base_url.clone(),
            auth: None,
        };
        let alarm_client = AlarmEventLogApiClient::new(FeignApiClient::new(api_config).await);

        let title = format!("Debezium 实例<{}>连接器异常", instance.name);
        let content = format!(
            "实例<{}>运行状态异常: {} ({}), 上游连接器<{}>, 下游连接器<{}>, Kafka Connect: {}",
            instance.name,
            run_status,
            run_status.value(),
            instance.source_connector_name,
            instance.sink_connector_name,
            instance.kafka_connect_url,
        );

        let add_dto = AlarmEventLogAddDto::builder()
            .event_code(event_code)
            .business_id(instance.id)
            .alert_id(U64(alert_id))
            .trigger_ms(U64(now_ms()))
            .title(title)
            .content(content)
            ._current_user_id(U64(1))
            .build();

        let ro = alarm_client.add(&add_dto).await?;
        info!("实例<{}>的异常告警上报成功: {}", instance.name, ro.msg);
        Ok(())
    }
}
