//! # 告警工具
//!
//! 连接器异常告警上报给报警中心（alarm-svr），报警中心再转给消息中心投递。
//!
//! ## 首次启动先把事件登记上去
//!
//! 报警中心只认识**登记过**的事件：`[alarm]` 三段（事件来源 / 报警级别 /
//! 事件策略）由 [`setup_dbz_alarm`] 在进程首次加载配置时按依赖顺序写进去。
//! 没登记就上报，会被报警中心以「查不到策略」拒收——之前那版要求运维先在
//! 报警中心手工建好、再把 `event-code` / `alert-id` 抄进配置，漏配就是静默
//! 不上报，而且 `alert-id` 还得指向一条已经存在的报警。
//!
//! ## 上报走 trigger，不走 event_log
//!
//! `alarm/alert/trigger` 才是报警中心的正规入口：它按事件策略算指纹、合并或
//! 新建报警、写事件日志、成立时发通知。直接往 `alarm_event_log` 插一条只是
//! 一行日志——报警列表里没有、也没有人收到通知（那张表的 add 就是通用 CRUD，
//! 没有钩子）。
//!
//! ## 上游、下游共用一条事件策略
//!
//! `business_id` 是连接器自己的主键，而两张连接器表的 ID 会撞。所以：
//!
//! - `business_id` 取「方向 + ID」的哈希（[`business_id_of`]），幂等键
//!   `(event_code, business_id, trigger_ms)` 才不会把上游 3 号与下游 3 号的
//!   同一毫秒上报判成同一条（同一轮巡检里两条上报只差几毫秒，真会撞）；
//! - 指纹取 `direction,connectorId` 两个 label，报警中心据此分开合并；
//! - 文案里的方向、名称、状态全部走 `labels`，由事件策略的模板渲染。
//!
//! Kafka Connect 本身不单独作为告警主体：一台集群不可达时，由挂在它下面的
//! 连接器告警表达，避免同一个根因换一个 `business_id` 再报一遍。
//!
//! ## 只补不覆盖
//!
//! 登记时每一条都是「先查，没有再加，撞唯一键也算已登记」：运营在报警中心
//! 改过的模板、合并窗口、提醒间隔、启用状态，dbz 重启后保持原样。

use crate::config::get_app_config;
use crate::config::{
    AlarmAlertLevelConfig, AlarmConfig, AlarmEventSourceConfig, AlarmPolicyConfig, alarm_queue_code,
};
use crate::dic::ConnectorRunStatus;
use crate::vo::{DbzKafkaConnectVo, DbzSinkConnectorVo, DbzSourceConnectorVo};
use alarm_api_client::dto::{
    AlarmAlertLevelAddDto, AlarmAlertLevelGroupAddDto, AlarmAlertLevelGroupQueryDto,
    AlarmAlertLevelQueryDto, AlarmEventPolicyAddDto, AlarmEventPolicyQueryDto,
    AlarmEventSourceAddDto, AlarmEventSourceQueryDto, AlarmEventTriggerDto,
    AlarmMessageQueueQueryDto,
};
use alarm_api_client::{
    AlarmAlertLevelApiClient, AlarmAlertLevelGroupApiClient, AlarmEventPolicyApiClient,
    AlarmEventSourceApiClient, AlarmMessageQueueApiClient, get_alarm_api_client,
};
use robotech::api::{RO_CODE_WARNING_DUPLICATE_KEY, U64};
use robotech::cst::user_id_cst::SYS_OPERATOR_USER_ID;
use std::collections::HashMap;
use tracing::{info, warn};
use wheel_rs::time_utils::now_ms;

/// # 告警工具
pub struct DbzAlarmUtils;

impl DbzAlarmUtils {
    /// # 上报上游连接器异常告警
    ///
    /// 当上游连接器的运行状态不为「运行中」时，向报警中心上报一条事件。
    ///
    /// ## 参数
    /// * `source` - 发生异常的上游连接器
    /// * `kafka_connect` - 运行该连接器的 Kafka Connect
    /// * `run_status` - 该连接器当前的运行状态
    ///
    /// ## 返回值
    /// * `Ok(())` - 上报成功（含被报警中心以业务码拒绝，那种情况只记日志）
    /// * `Err(anyhow::Error)` - 报警中心不可达或返回错误
    pub async fn send_source_connector_alarm(
        source: &DbzSourceConnectorVo,
        kafka_connect: &DbzKafkaConnectVo,
        run_status: ConnectorRunStatus,
    ) -> Result<(), anyhow::Error> {
        send(&ConnectorAlarmSubject {
            direction: Direction::Source,
            connector_id: source.id.0,
            connector_name: &source.name,
            connector_name_on_cluster: &source.connector_name,
            run_status,
            kafka_connect_name: &kafka_connect.name,
            kafka_connect_url: &kafka_connect.url,
            // 上游没有上游，模板又没有条件判断，所以这一段先拼成短语
            relation: "无上游连接器",
        })
        .await
    }

    /// # 上报下游连接器异常告警
    ///
    /// 当下游连接器的运行状态不为「运行中」时，向报警中心上报一条事件。
    ///
    /// ## 参数
    /// * `sink` - 发生异常的下游连接器
    /// * `source_name` - 该下游连接器所属上游连接器的名称（仅用于文案定位）
    /// * `kafka_connect` - 运行该连接器的 Kafka Connect
    /// * `run_status` - 该连接器当前的运行状态
    ///
    /// ## 返回值
    /// * `Ok(())` - 上报成功（含被报警中心以业务码拒绝，那种情况只记日志）
    /// * `Err(anyhow::Error)` - 报警中心不可达或返回错误
    pub async fn send_sink_connector_alarm(
        sink: &DbzSinkConnectorVo,
        source_name: &str,
        kafka_connect: &DbzKafkaConnectVo,
        run_status: ConnectorRunStatus,
    ) -> Result<(), anyhow::Error> {
        send(&ConnectorAlarmSubject {
            direction: Direction::Sink,
            connector_id: sink.id.0,
            connector_name: &sink.name,
            connector_name_on_cluster: &sink.connector_name,
            run_status,
            kafka_connect_name: &kafka_connect.name,
            kafka_connect_url: &kafka_connect.url,
            relation: &format!("上游连接器<{source_name}>"),
        })
        .await
    }
}

/// # 连接器的方向
///
/// 上游与下游共用一条事件策略，方向既要进指纹（不然两张表的同号 ID 会被合
/// 并），也要进文案（「下游连接器异常」和「上游连接器异常」是两回事）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Direction {
    /// 上游（Debezium source connector）
    Source,
    /// 下游（Debezium sink connector）
    Sink,
}

impl Direction {
    /// 指纹与 `business_id` 里的方向标签
    ///
    /// 改名会让报警"换一条"：指纹与幂等键都变了，旧报警要等窗口到期才收尾。
    const fn code(self) -> &'static str {
        match self {
            Direction::Source => "source",
            Direction::Sink => "sink",
        }
    }

    /// 文案里的方向（`{{directionName}}`）
    const fn name(self) -> &'static str {
        match self {
            Direction::Source => "上游",
            Direction::Sink => "下游",
        }
    }
}

/// # 一条连接器异常告警的主体信息
///
/// 上游与下游除方向、名称、上游关系之外完全一样，所以组装文案变量（`labels`）
/// 的部分共用这一份，两端只在构造时填不同字段。
///
/// ## labels 是模板变量，也参与指纹
///
/// 事件策略的标题/内容模板从这里取变量（模板是运营可改的，dbz 只保证变量都在）。
/// 缺失的变量会在文案里留下 `{xxx}` 占位符，所以这些键一个都不能少——
/// 测试 `content_template_variables_all_present` 会跟着默认模板一起校验。
struct ConnectorAlarmSubject<'a> {
    /// 方向（同时决定 `directionName`，也是指纹的一部分）
    direction: Direction,
    /// 连接器 ID（指纹用，文案里也带上便于排查）
    connector_id: u64,
    /// 连接器的业务名称
    connector_name: &'a str,
    /// Debezium 里的连接器名（`connector_name` 列）
    connector_name_on_cluster: &'a str,
    /// 当前运行状态
    run_status: ConnectorRunStatus,
    /// 所属 Kafka Connect 名称（定位集群用）
    kafka_connect_name: &'a str,
    /// 所属 Kafka Connect 地址（定位集群用）
    kafka_connect_url: &'a str,
    /// 上游关系短语：上游填「无上游连接器」，下游填「上游连接器<xxx>」
    relation: &'a str,
}

impl ConnectorAlarmSubject<'_> {
    /// # 拼上报用的 labels
    ///
    /// 键名用 camelCase，模板里按 `{{key}}` 取。`direction` 与 `connectorId` 同时
    /// 是事件策略的 `finger_print_labels`（见 `[alarm.policy]` 的默认值）。
    fn to_labels(&self) -> HashMap<String, String> {
        let mut labels = HashMap::with_capacity(11);
        labels.insert("direction".to_string(), self.direction.code().to_string());
        labels.insert(
            "directionName".to_string(),
            self.direction.name().to_string(),
        );
        labels.insert("connectorId".to_string(), self.connector_id.to_string());
        labels.insert("connectorName".to_string(), self.connector_name.to_string());
        labels.insert(
            "connectorNameOnCluster".to_string(),
            self.connector_name_on_cluster.to_string(),
        );
        labels.insert("runStatus".to_string(), self.run_status.to_string());
        labels.insert(
            "runStatusName".to_string(),
            run_status_name(self.run_status).to_string(),
        );
        labels.insert(
            "runStatusValue".to_string(),
            self.run_status.value().to_string(),
        );
        labels.insert(
            "kafkaConnectName".to_string(),
            self.kafka_connect_name.to_string(),
        );
        labels.insert(
            "kafkaConnectUrl".to_string(),
            self.kafka_connect_url.to_string(),
        );
        labels.insert("relation".to_string(), self.relation.to_string());
        labels
    }
}

/// # 运行状态的中文名
///
/// `ConnectorRunStatus` 的 `Display` 是 kebab-case（`kafka-connect-failed`），
/// 直接进中文报警文案读起来是半截英文。字典只提供了 `value()`，所以中文名在
/// 这里按枚举补一份——加变体时编译器会在这里报缺失，不会漏。
const fn run_status_name(status: ConnectorRunStatus) -> &'static str {
    match status {
        ConnectorRunStatus::None => "无状态",
        ConnectorRunStatus::Running => "运行中",
        ConnectorRunStatus::Disabled => "已禁用",
        ConnectorRunStatus::KafkaConnectFailed => "连接 Kafka Connect 失败",
        ConnectorRunStatus::RegisterFailed => "注册连接器失败",
        ConnectorRunStatus::CheckFailed => "检测连接器失败",
        ConnectorRunStatus::CheckError => "检测连接器异常",
        ConnectorRunStatus::CheckStatusFailed => "检测连接器状态失败",
        ConnectorRunStatus::CheckStatusError => "检测连接器状态异常",
    }
}

/// # 触发一条连接器异常事件
///
/// ## 参数
///
/// - `subject`：告警主体，提供模板变量（`labels`）与幂等键里的 `business_id`
///
/// ## 返回值
///
/// - 报警中心接收（含合并进已有报警）返回 `Ok(())`；
/// - 报警中心以业务码拒绝（策略不存在、队列没登记）只记 `warn`，不向上报错：
///   调用方是巡检循环，一条日志类问题不该让整轮检查失败；
/// - 网络/解析失败返回 `Err`，由调用方决定怎么记。
async fn send(subject: &ConnectorAlarmSubject<'_>) -> Result<(), anyhow::Error> {
    let Ok(api_client) = get_alarm_api_client() else {
        // 没配 `[api.alarm]`：告警是选配功能，启动时已经明确提示过一次，
        // 这里不再每轮巡检刷一条 warn
        return Ok(());
    };
    let event_code = get_app_config()?.alarm.policy.event_code.clone();

    let dto = AlarmEventTriggerDto {
        event_code,
        business_id: business_id_of(subject.direction, subject.connector_id),
        // 巡检发现异常的时刻就是事件时刻。每轮都用新值：同一轮里的重复上报才
        // 该被当成重投（那种情况由报警中心的幂等键和合并窗口接住）。
        trigger_ms: now_ms(),
        labels: subject.to_labels(),
        annotations: HashMap::new(),
        // 文案由事件策略的模板渲染，这里不重复给一份
        title: None,
        content: None,
        ..Default::default()
    };

    let ro = api_client.alert.trigger(&dto, SYS_OPERATOR_USER_ID).await?;
    if ro.is_ok() {
        info!(
            "连接器异常告警已上报: direction={}, connector_id={}, alert_id={:?}",
            subject.direction.code(),
            subject.connector_id,
            ro.extra.as_ref().map(|vo| vo.id.0),
        );
    } else {
        warn!(
            "连接器异常告警被报警中心拒绝（策略或队列没登记？先看 setup_dbz_alarm 的启动日志）: {ro:?}"
        );
    }
    Ok(())
}

/// # 连接器 → 报警的 `business_id`
///
/// FNV-1a 64：确定性、无依赖、分布够用。为什么不用裸 ID：
///
/// 1. `business_id` 与事件编码、触发时间戳一起构成幂等键。两张连接器表各有
///    自己的主键，上游 3 号与下游 3 号会算出同一个键——同一轮巡检里两条上报
///    只隔几毫秒，撞上就会把下游那条当成重投直接吞掉（报警少一条、次数也不
///    涨）。加上方向前缀后两者天然分开；
/// 2. 同一连接器的 ID 恒定，幂等键才有意义；
/// 3. 非 0——报警中心把 `business_id == 0` 当作「没给业务 ID」直接拒收。
///
/// 不裁剪高位：报警中心那列是 `int8`，高位为 1 的值会以负数落库，但读写走
/// 同一套位模式（`as i64` / `as u64` 双射），取出来仍是原值。这是哈希值，
/// 正负不承载语义，按它排序本来就是乱序，没理由把 64 位砍成 63 位。
///
/// 哈希不可读，排查时看报警的 `labels.connectorId`（原始 ID 就在那里）。
fn business_id_of(direction: Direction, connector_id: u64) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = FNV_OFFSET_BASIS;
    for byte in format!("{}:{connector_id}", direction.code()).as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }

    // 0 会被报警中心当成「没给业务 ID」
    hash.max(1)
}

/// # 登记连接器异常事件（启动时调用）
///
/// 按「级别分组 → 报警级别 → 事件来源 → 事件策略」的顺序登记，后一步要用前
/// 一步的 ID。
///
/// ## 参数
///
/// - `config`：`[alarm]` 配置段。
///
/// ## 返回值
///
/// - `Ok(())`：登记完成（含「已登记过」）；
/// - `Err(anyhow::Error)`：报警中心不可用、配置缺失、级别队列不存在、接口返回错误。
///
/// 调用方（`main.rs`）用 `?` 把错误抛出去结束启动：登记不上就等于连接器挂了
/// 没人知道，不该让进程带着「静默缺功能」的状态跑下去。
///
/// ## 每次启动都跑
///
/// 登记本身幂等，调用方只在首次加载配置时触发（`changed.is_none()`）。热更新
/// 不重跑：事件定义不是运行期参数，改了要重启。
pub async fn setup_dbz_alarm(config: &AlarmConfig) -> anyhow::Result<()> {
    let api_client = get_alarm_api_client()?;

    let alert_level_group_id =
        register_alert_level_group(&api_client.alert_level_group, &config.alert_level).await?;
    let alert_level_id = register_alert_level(
        &api_client.alert_level,
        &config.alert_level,
        alert_level_group_id,
    )
    .await?;
    let event_source_id =
        register_event_source(&api_client.event_source, &config.event_source).await?;

    // 队列是报警中心的种子数据，按级别值拼编码去查，查不到就没法给策略挂外键
    let queue_code = alarm_queue_code(config.alert_level.value);
    let Some(message_queue_id) =
        get_message_queue_id(&api_client.message_queue, &queue_code).await?
    else {
        anyhow::bail!(
            "报警中心没有消息队列 {}，连接器异常事件登记不上：报警收得下但通知发不出去。\
             先启动 alarm-svr 让它把 1~5 级队列种好，再启动 dbz",
            queue_code
        );
    };

    register_event_policy(
        &api_client.event_policy,
        &config.policy,
        event_source_id,
        alert_level_id,
        message_queue_id,
    )
    .await?;

    info!(
        "连接器异常事件已登记到报警中心: event_code={}, level={}, source={}, queue={}",
        config.policy.event_code, config.alert_level.code, config.event_source.code, queue_code
    );
    Ok(())
}

/// # 登记报警级别分组，返回记录 ID
///
/// 分组是报警中心的共享字典，迁移脚本已经种好了本行业常用的几组：先按
/// `code` 查，查到直接复用，不白跑一次插入。
async fn register_alert_level_group(
    api_client: &AlarmAlertLevelGroupApiClient,
    config: &AlarmAlertLevelConfig,
) -> anyhow::Result<u64> {
    if let Some(id) =
        query_alert_level_group_id(api_client, &config.group_code, &config.group_name).await?
    {
        info!("报警级别分组已登记，复用: {}", config.group_code);
        return Ok(id);
    }

    let add_dto = AlarmAlertLevelGroupAddDto {
        code: Some(config.group_code.clone()),
        name: Some(config.group_name.clone()),
        remark: Some(config.group_remark.clone()),
        _current_user_id: U64(SYS_OPERATOR_USER_ID),
        ..Default::default()
    };

    match api_client.add(&add_dto).await {
        Ok(ro) if ro.is_ok() => ro
            .extra
            .map(|vo| vo.id.0)
            .ok_or_else(|| anyhow::anyhow!("报警级别分组 {} 登记后未返回记录", config.group_code)),
        Ok(ro) if ro.is_warn_code(RO_CODE_WARNING_DUPLICATE_KEY) => {
            // 并发里别人刚建好（或撞的是名称唯一键），再查一次拿 ID
            query_alert_level_group_id(api_client, &config.group_code, &config.group_name)
                .await?
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "报警级别分组 {} 已登记但按编码和名称都查不到记录",
                        config.group_code
                    )
                })
        }
        Ok(ro) => anyhow::bail!("报警级别分组 {} 登记失败: {ro:?}", config.group_code),
        Err(e) => anyhow::bail!("报警级别分组 {} 登记失败: {e:?}", config.group_code),
    }
}

/// # 按编码（失败再按名称）查报警级别分组 ID
///
/// `alarm_alert_level_group` 的 `code` 与 `name` 都是唯一键：按编码查不到但按
/// 名称查到，说明运营手工建过一条同名记录，沿用即可——它就是要挂的那条。
async fn query_alert_level_group_id(
    api_client: &AlarmAlertLevelGroupApiClient,
    code: &str,
    name: &str,
) -> anyhow::Result<Option<u64>> {
    let ro = api_client
        .get_by_query_dto(
            &AlarmAlertLevelGroupQueryDto {
                code: Some(code.to_string()),
                ..Default::default()
            },
            SYS_OPERATOR_USER_ID,
        )
        .await
        .map_err(|e| anyhow::anyhow!("查询报警级别分组 {code} 失败: {e:?}"))?;
    if let Some(vo) = ro.extra {
        return Ok(Some(vo.id.0));
    }

    let ro = api_client
        .get_by_query_dto(
            &AlarmAlertLevelGroupQueryDto {
                name: Some(name.to_string()),
                ..Default::default()
            },
            SYS_OPERATOR_USER_ID,
        )
        .await
        .map_err(|e| anyhow::anyhow!("按名称查询报警级别分组 {name} 失败: {e:?}"))?;
    Ok(ro.extra.map(|vo| vo.id.0))
}

/// # 登记报警级别，返回记录 ID
///
/// 级别只按 `code` 查：`name` 在级别表里不是唯一键（不同分组可以有同名的
/// 级别），按名称回查会挂错分组。
async fn register_alert_level(
    api_client: &AlarmAlertLevelApiClient,
    config: &AlarmAlertLevelConfig,
    alert_level_group_id: u64,
) -> anyhow::Result<u64> {
    if let Some(id) = query_alert_level_id(api_client, &config.code).await? {
        info!("报警级别已登记，复用: {}", config.code);
        return Ok(id);
    }

    let add_dto = AlarmAlertLevelAddDto {
        alert_level_group_id: Some(U64(alert_level_group_id)),
        code: Some(config.code.clone()),
        name: Some(config.name.clone()),
        value: Some(config.value),
        remark: Some(config.remark.clone()),
        _current_user_id: U64(SYS_OPERATOR_USER_ID),
        ..Default::default()
    };

    match api_client.add(&add_dto).await {
        Ok(ro) if ro.is_ok() => ro
            .extra
            .map(|vo| vo.id.0)
            .ok_or_else(|| anyhow::anyhow!("报警级别 {} 登记后未返回记录", config.code)),
        Ok(ro) if ro.is_warn_code(RO_CODE_WARNING_DUPLICATE_KEY) => {
            // 撞的可能是「分组 + 值」这个复合唯一键：同组里已经有一个同值的
            // 级别（编码不同）。按编码查不到就报错，让运维去对齐配置。
            query_alert_level_id(api_client, &config.code)
                .await?
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "报警级别 {} 登记时撞唯一键，但按编码查不到记录；\
                         多半是分组 {} 里已经有一个值为 {} 的级别，改 value 或改 code",
                        config.code,
                        config.group_code,
                        config.value
                    )
                })
        }
        Ok(ro) => anyhow::bail!("报警级别 {} 登记失败: {ro:?}", config.code),
        Err(e) => anyhow::bail!("报警级别 {} 登记失败: {e:?}", config.code),
    }
}

/// # 按编码查报警级别 ID
async fn query_alert_level_id(
    api_client: &AlarmAlertLevelApiClient,
    code: &str,
) -> anyhow::Result<Option<u64>> {
    let ro = api_client
        .get_by_query_dto(
            &AlarmAlertLevelQueryDto {
                code: Some(code.to_string()),
                ..Default::default()
            },
            SYS_OPERATOR_USER_ID,
        )
        .await
        .map_err(|e| anyhow::anyhow!("查询报警级别 {code} 失败: {e:?}"))?;
    Ok(ro.extra.map(|vo| vo.id.0))
}

/// # 登记事件来源，返回记录 ID
///
/// 事件来源的 `code` 与消息中心的 `msg_event_source.code` 是同一个值：报警
/// 中心同步来源时按编码判重，两边一致才不会在消息中心堆出两个来源。
async fn register_event_source(
    api_client: &AlarmEventSourceApiClient,
    config: &AlarmEventSourceConfig,
) -> anyhow::Result<u64> {
    if let Some(id) = query_event_source_id(api_client, &config.code, &config.name).await? {
        info!("事件来源已登记，复用: {}", config.code);
        return Ok(id);
    }

    let add_dto = AlarmEventSourceAddDto {
        code: Some(config.code.clone()),
        name: Some(config.name.clone()),
        remark: Some(config.remark.clone()),
        _current_user_id: U64(SYS_OPERATOR_USER_ID),
        ..Default::default()
    };

    match api_client.add(&add_dto).await {
        Ok(ro) if ro.is_ok() => ro
            .extra
            .map(|vo| vo.id.0)
            .ok_or_else(|| anyhow::anyhow!("事件来源 {} 登记后未返回记录", config.code)),
        Ok(ro) if ro.is_warn_code(RO_CODE_WARNING_DUPLICATE_KEY) => {
            query_event_source_id(api_client, &config.code, &config.name)
                .await?
                .ok_or_else(|| {
                    anyhow::anyhow!("事件来源 {} 已登记但按编码和名称都查不到记录", config.code)
                })
        }
        Ok(ro) => anyhow::bail!("事件来源 {} 登记失败: {ro:?}", config.code),
        Err(e) => anyhow::bail!("事件来源 {} 登记失败: {e:?}", config.code),
    }
}

/// # 按编码（失败再按名称）查事件来源 ID
async fn query_event_source_id(
    api_client: &AlarmEventSourceApiClient,
    code: &str,
    name: &str,
) -> anyhow::Result<Option<u64>> {
    let ro = api_client
        .get_by_query_dto(
            &AlarmEventSourceQueryDto {
                code: Some(code.to_string()),
                ..Default::default()
            },
            SYS_OPERATOR_USER_ID,
        )
        .await
        .map_err(|e| anyhow::anyhow!("查询事件来源 {code} 失败: {e:?}"))?;
    if let Some(vo) = ro.extra {
        return Ok(Some(vo.id.0));
    }

    let ro = api_client
        .get_by_query_dto(
            &AlarmEventSourceQueryDto {
                name: Some(name.to_string()),
                ..Default::default()
            },
            SYS_OPERATOR_USER_ID,
        )
        .await
        .map_err(|e| anyhow::anyhow!("按名称查询事件来源 {name} 失败: {e:?}"))?;
    Ok(ro.extra.map(|vo| vo.id.0))
}

/// # 按编码查报警消息队列 ID
///
/// 队列由报警中心的迁移脚本种好，dbz 只读不写。查不到返回 `Ok(None)`，由
/// 调用方决定怎么处理。
async fn get_message_queue_id(
    api_client: &AlarmMessageQueueApiClient,
    code: &str,
) -> anyhow::Result<Option<u64>> {
    let ro = api_client
        .get_by_query_dto(
            &AlarmMessageQueueQueryDto {
                code: Some(code.to_string()),
                ..Default::default()
            },
            SYS_OPERATOR_USER_ID,
        )
        .await
        .map_err(|e| anyhow::anyhow!("查询报警消息队列 {code} 失败: {e:?}"))?;
    Ok(ro.extra.map(|vo| vo.id.0))
}

/// # 登记连接器异常事件策略
///
/// 已登记即跳过，**不覆盖**：模板、合并窗口、提醒间隔、启用状态都是运营数据，
/// dbz 只在库里没有这条事件时补一份默认配置上去。策略写入后报警中心的
/// `after_write` 钩子会把它同步到消息中心。
async fn register_event_policy(
    api_client: &AlarmEventPolicyApiClient,
    config: &AlarmPolicyConfig,
    event_source_id: u64,
    alert_level_id: u64,
    message_queue_id: u64,
) -> anyhow::Result<()> {
    let add_dto = AlarmEventPolicyAddDto {
        event_code: Some(config.event_code.clone()),
        name: Some(config.name.clone()),
        finger_print_labels: Some(config.finger_print_labels.clone()),
        title_template: Some(config.title_template.clone()),
        content_template: Some(config.content_template.clone()),
        event_source_id: Some(U64(event_source_id)),
        alert_level_id: Some(U64(alert_level_id)),
        message_queue_id: Some(U64(message_queue_id)),
        repeat_interval_ms: Some(config.repeat_interval_ms.map(U64)),
        merge_window_ms: Some(config.merge_window_ms.map(U64)),
        debounce_count: Some(config.debounce_count.map(U64)),
        debounce_window_ms: Some(config.debounce_window_ms.map(U64)),
        remark: Some(config.remark.clone()),
        enabled: Some(config.enabled),
        _current_user_id: U64(SYS_OPERATOR_USER_ID),
        ..Default::default()
    };

    match api_client.add(&add_dto).await {
        Ok(ro) if ro.is_ok() => {
            info!(
                "登记连接器异常事件策略成功，报警中心会同步给消息中心: {}",
                config.event_code
            );
            Ok(())
        }
        Ok(ro) if ro.is_warn_code(RO_CODE_WARNING_DUPLICATE_KEY) => {
            match query_event_policy_id(api_client, &config.event_code, &config.name).await? {
                Some(_) => {
                    info!(
                        "连接器异常事件策略已登记，保留报警中心里的现有配置: {}",
                        config.event_code
                    );
                    Ok(())
                }
                None => anyhow::bail!(
                    "连接器异常事件策略 {} 登记时撞唯一键，但按编码和名称都查不到记录",
                    config.event_code
                ),
            }
        }
        Ok(ro) => anyhow::bail!("连接器异常事件策略 {} 登记失败: {ro:?}", config.event_code),
        Err(e) => anyhow::bail!("连接器异常事件策略 {} 登记失败: {e:?}", config.event_code),
    }
}

/// # 按事件编码（失败再按名称）查事件策略 ID
///
/// 按名称命中要告警：策略表的 `name` 也是唯一键，运营手工建过一条同名但编码
/// 不同的事件时，插入会撞名称而不是编码。沿用已有记录比启动失败好，但要提示
/// 编码不一致——否则 dbz 上报的 `event_code` 永远匹配不到策略。
async fn query_event_policy_id(
    api_client: &AlarmEventPolicyApiClient,
    event_code: &str,
    name: &str,
) -> anyhow::Result<Option<u64>> {
    let ro = api_client
        .get_by_query_dto(
            &AlarmEventPolicyQueryDto {
                event_code: Some(event_code.to_string()),
                ..Default::default()
            },
            SYS_OPERATOR_USER_ID,
        )
        .await
        .map_err(|e| anyhow::anyhow!("查询事件策略 {event_code} 失败: {e:?}"))?;
    if let Some(vo) = ro.extra {
        return Ok(Some(vo.id.0));
    }

    let ro = api_client
        .get_by_query_dto(
            &AlarmEventPolicyQueryDto {
                name: Some(name.to_string()),
                ..Default::default()
            },
            SYS_OPERATOR_USER_ID,
        )
        .await
        .map_err(|e| anyhow::anyhow!("按名称查询事件策略 {name} 失败: {e:?}"))?;
    if let Some(vo) = ro.extra {
        warn!(
            "事件策略「{name}」已存在但编码不是 {event_code}，沿用已有记录；\
             请把库里或配置里的事件编码统一，否则连接器告警会被报警中心以「未注册」拒收"
        );
        return Ok(Some(vo.id.0));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn subject(direction: Direction) -> ConnectorAlarmSubject<'static> {
        ConnectorAlarmSubject {
            direction,
            connector_id: 3,
            connector_name: "订单同步",
            connector_name_on_cluster: "order_sync",
            run_status: ConnectorRunStatus::CheckError,
            kafka_connect_name: "生产集群",
            kafka_connect_url: "http://kafka-connect:8083",
            relation: "上游连接器<订单源>",
        }
    }

    /// 同一连接器每次上报的 business_id 必须一样，幂等键才成立
    #[test]
    fn business_id_is_stable_per_connector() {
        assert_eq!(
            business_id_of(Direction::Source, 7),
            business_id_of(Direction::Source, 7)
        );
        assert_ne!(
            business_id_of(Direction::Source, 7),
            business_id_of(Direction::Source, 8)
        );
    }

    /// 上游 3 号与下游 3 号必须分开：两张连接器表各有自己的主键，撞上就会被
    /// 当成重投吞掉下游那条上报
    #[test]
    fn business_id_differs_by_direction() {
        assert_ne!(
            business_id_of(Direction::Source, 3),
            business_id_of(Direction::Sink, 3)
        );
        // 不能是 0：报警中心把它当作「没给业务 ID」
        assert_ne!(business_id_of(Direction::Source, 0), 0);
    }

    /// 指纹取 direction + connectorId，缺了方向就会被合并成一条
    #[test]
    fn finger_print_labels_covers_direction() {
        let labels = subject(Direction::Sink).to_labels();
        assert_eq!(labels.get("direction").map(String::as_str), Some("sink"));
        assert_eq!(labels.get("connectorId").map(String::as_str), Some("3"));

        let policy = AlarmPolicyConfig::default();
        let finger_print_labels = policy.finger_print_labels.unwrap();
        for key in finger_print_labels.split(',') {
            assert!(
                labels.contains_key(key),
                "指纹标签 {key} 没有对应的 label，报警中心算不出指纹"
            );
        }
    }

    /// 内容模板里的变量必须都在 labels 里：模板没有条件判断，缺变量会留下
    /// `{{xxx}}` 占位符
    #[test]
    fn content_template_variables_all_present() {
        let labels = subject(Direction::Source).to_labels();

        let policy = AlarmPolicyConfig::default();
        let title = policy.title_template.unwrap();
        for key in template_variables(&title) {
            assert!(
                labels.contains_key(key),
                "标题模板变量 {key} 在 labels 里不存在"
            );
        }
        let content = policy.content_template.unwrap();
        for key in template_variables(&content) {
            assert!(
                labels.contains_key(key),
                "内容模板变量 {key} 在 labels 里不存在，文案会留下占位符"
            );
        }
    }

    /// 取出 `{{key}}` 形式的模板变量（`{{key:json}}` 也算同一个 key）
    fn template_variables(template: &str) -> Vec<&str> {
        template
            .split("{{")
            .skip(1)
            .filter_map(|seg| seg.split("}}").next())
            .map(|key| key.split(':').next().unwrap_or(key))
            .collect()
    }
}
