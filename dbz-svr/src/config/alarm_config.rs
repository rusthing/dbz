//! # 告警配置
//!
//! 对应配置文件中的 `[alarm]` 配置段。三段各对应报警中心里的一组表：
//!
//! ```text
//! event_source → alarm_event_source                （事件来源）
//! alert_level  → alarm_alert_level_group + alarm_alert_level（级别与所属分组）
//! policy       → alarm_event_policy                （事件本身：模板、合并窗口、提醒节奏）
//! ```
//!
//! 首次启动时由 [`crate::utils::dbz_alarm_utils::setup_dbz_alarm`] 按这个顺序
//! 登记上去，之后每次重启只补不改：运营在报警中心改过的模板、窗口、启用状态
//! 不会因为 dbz 重启被冲掉。报警中心收下策略后会把它同步给消息中心，dbz 不
//! 需要自己往消息中心登记什么。
//!
//! ## 级别与队列都是共享字典
//!
//! 报警级别分组、级别、消息队列都由报警中心的迁移脚本种好，dbz 只是按编码
//! 复用其中一条，不新建：级别用 `industrial_control` 组的 2 级「严重」——
//! 连接器挂掉是「功能严重受损，需尽快处理」，但服务本身还在跑，不到 1 级
//! 「紧急」（那是 dtw 的 SOS 用的）。
//!
//! 队列不在这里配：级别值 `value` 与队列是一对一约定，按
//! `msg.publish.alarm.{value}` 拼出编码（见 [`alarm_queue_code`]），改级别就
//! 等于改队列，多一个配置项只会让两者不同步。

use serde::{Deserialize, Serialize};

/// # 告警配置
///
/// 对应配置文件中的 `[alarm]` 配置段。所有字段都有默认值，配置里只写需要
/// 覆盖的部分。
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct AlarmConfig {
    /// 事件来源（dbz 的告警都挂在这个来源下）
    #[serde(default = "alarm_event_source_default")]
    pub event_source: AlarmEventSourceConfig,
    /// 报警级别（含所属分组）
    #[serde(default = "alarm_alert_level_default")]
    pub alert_level: AlarmAlertLevelConfig,
    /// 连接器异常事件策略
    #[serde(default = "alarm_policy_default")]
    pub policy: AlarmPolicyConfig,
}

/// # 报警事件来源
///
/// 对应 `alarm_event_source`，`code` 与 `name` 都是唯一键。`code` 同时是报警
/// 中心向消息中心登记来源时用的编码，两边必须一致，否则消息中心里会堆出两个
/// 来源。
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct AlarmEventSourceConfig {
    /// 编码
    #[serde(default = "alarm_event_source_code_default")]
    pub code: String,
    /// 名称
    #[serde(default = "alarm_event_source_name_default")]
    pub name: String,
    /// 备注
    #[serde(default = "alarm_event_source_remark_default")]
    pub remark: Option<String>,
}

/// # 报警级别
///
/// 对应 `alarm_alert_level`，以及它所属的 `alarm_alert_level_group`。级别按
/// 分组维护，同一分组内的 `value` 唯一且值越小越严重；分组只用于归类与展示，
/// 不参与路由。
///
/// 登记时先按 `group-code` 找分组（没有就建），再按 `code` 找级别（没有就建），
/// 所以这里要同时给出分组与级别两段信息。
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct AlarmAlertLevelConfig {
    /// 分组编码
    #[serde(default = "alarm_level_group_code_default")]
    pub group_code: String,
    /// 分组名称
    #[serde(default = "alarm_level_group_name_default")]
    pub group_name: String,
    /// 分组备注
    #[serde(default = "alarm_level_group_remark_default")]
    pub group_remark: Option<String>,
    /// 编码
    #[serde(default = "alarm_level_code_default")]
    pub code: String,
    /// 名称
    #[serde(default = "alarm_level_name_default")]
    pub name: String,
    /// 值（同组内唯一，1 最严重；报警队列按它拼主题）
    #[serde(default = "alarm_level_value_default")]
    pub value: i16,
    /// 备注
    #[serde(default = "alarm_level_remark_default")]
    pub remark: Option<String>,
}

/// # 连接器异常事件策略
///
/// 对应 `alarm_event_policy`。除三段外键（来源 / 级别 / 消息队列）外，其余都是
/// 报警中心决定「怎么合并、怎么提醒、长什么样」的运营参数。
///
/// ## 上游与下游共用一条策略
///
/// 两类连接器走同一个 `event_code`，靠 `labels` 里的 `direction` 区分：
/// `finger_print_labels` 取 `direction,connectorId`，两者一起才是「哪一个
/// 连接器」。少了 `direction` 会出事——`business_id` 是连接器自己的主键，两张
/// 连接器表的 ID 会撞，撞上就会被当成同一条报警合并。
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct AlarmPolicyConfig {
    /// 事件编码（唯一，上报时按它查策略）
    #[serde(default = "connector_error_event_code_default")]
    pub event_code: String,
    /// 事件名称（唯一，也是消息中心里消息模板的名称）
    #[serde(default = "connector_error_event_name_default")]
    pub name: String,
    /// 指纹标签（逗号分隔的 labels 键名）
    #[serde(default = "connector_error_finger_print_labels_default")]
    pub finger_print_labels: Option<String>,
    /// 标题模板
    #[serde(default = "connector_error_title_template_default")]
    pub title_template: Option<String>,
    /// 内容模板
    #[serde(default = "connector_error_content_template_default")]
    pub content_template: Option<String>,
    /// 重复提醒间隔（毫秒）
    ///
    /// 异常一直没恢复时按这个间隔重复提醒。取 30 分钟：连接器挂掉是「数据不再
    /// 同步」的持续故障，值太大会让人以为早好了，太小会盖住其他报警。
    #[serde(default = "connector_error_repeat_interval_ms_default")]
    pub repeat_interval_ms: Option<u64>,
    /// 合并窗口（毫秒）
    ///
    /// 巡检每 60 秒跑一次，异常持续时每轮都会上报一次。窗口内（判据是两次触发
    /// 的时间差）只涨触发次数、不新建报警也不重复通知；超过窗口才把旧报警置为
    /// 自动恢复、重新建一条并通知。取 5 分钟：既能把一分钟一次的巡检压成一条，
    /// 又能在连接器恢复后 5 分钟内自动收尾。
    #[serde(default = "connector_error_merge_window_ms_default")]
    pub merge_window_ms: Option<u64>,
    /// 防抖次数
    #[serde(default)]
    pub debounce_count: Option<u64>,
    /// 防抖窗口（毫秒）
    #[serde(default)]
    pub debounce_window_ms: Option<u64>,
    /// 备注
    #[serde(default = "connector_error_remark_default")]
    pub remark: Option<String>,
    /// 是否启用
    #[serde(default = "enabled_default")]
    pub enabled: bool,
}

fn alarm_event_source_default() -> AlarmEventSourceConfig {
    AlarmEventSourceConfig {
        code: alarm_event_source_code_default(),
        name: alarm_event_source_name_default(),
        remark: alarm_event_source_remark_default(),
    }
}

impl Default for AlarmEventSourceConfig {
    fn default() -> Self {
        alarm_event_source_default()
    }
}

fn alarm_alert_level_default() -> AlarmAlertLevelConfig {
    AlarmAlertLevelConfig {
        group_code: alarm_level_group_code_default(),
        group_name: alarm_level_group_name_default(),
        group_remark: alarm_level_group_remark_default(),
        code: alarm_level_code_default(),
        name: alarm_level_name_default(),
        value: alarm_level_value_default(),
        remark: alarm_level_remark_default(),
    }
}

impl Default for AlarmAlertLevelConfig {
    fn default() -> Self {
        alarm_alert_level_default()
    }
}

fn alarm_policy_default() -> AlarmPolicyConfig {
    AlarmPolicyConfig {
        event_code: connector_error_event_code_default(),
        name: connector_error_event_name_default(),
        finger_print_labels: connector_error_finger_print_labels_default(),
        title_template: connector_error_title_template_default(),
        content_template: connector_error_content_template_default(),
        repeat_interval_ms: connector_error_repeat_interval_ms_default(),
        merge_window_ms: connector_error_merge_window_ms_default(),
        debounce_count: None,
        debounce_window_ms: None,
        remark: connector_error_remark_default(),
        enabled: enabled_default(),
    }
}

impl Default for AlarmPolicyConfig {
    fn default() -> Self {
        alarm_policy_default()
    }
}

/// 事件来源编码
///
/// 与报警中心向消息中心登记来源时用的编码是同一个值。
const ALARM_EVENT_SOURCE_CODE_DEFAULT: &str = "dbz";
/// 事件来源名称
const ALARM_EVENT_SOURCE_NAME_DEFAULT: &str = "数据同步";
/// 事件来源备注
const ALARM_EVENT_SOURCE_REMARK_DEFAULT: &str = "dbz 的 Debezium 连接器与数据同步状态";
/// 报警级别分组的编码
///
/// 级别是报警中心的共享字典，按分组维护。dbz 用「工业自动化监控」这一组，它
/// 是报警中心迁移脚本里已经种好的工业场景五级告警，这里只是按编码复用。
const ALARM_LEVEL_GROUP_CODE_DEFAULT: &str = "industrial_control";
/// 报警级别分组的名称
const ALARM_LEVEL_GROUP_NAME_DEFAULT: &str = "工业自动化监控";
/// 报警级别分组的备注
const ALARM_LEVEL_GROUP_REMARK_DEFAULT: &str = "工业/自动化/监控系统通用的五级告警，1 级最严重";
/// 报警级别编码
///
/// 编码与名称都不带来源前缀：同一级别多个发布方共用。
const ALARM_LEVEL_CODE_DEFAULT: &str = "industrial_control_2";
/// 报警级别名称
const ALARM_LEVEL_NAME_DEFAULT: &str = "严重";
/// 报警级别的值
///
/// 2 = 功能严重受损，需尽快处理。队列主题按它拼出来
/// （`msg.publish.alarm.2`），所以改这个值等于把报警改投到另一条队列上。
const ALARM_LEVEL_VALUE_DEFAULT: i16 = 2;
/// 报警级别备注
const ALARM_LEVEL_REMARK_DEFAULT: &str = "功能严重受损，需尽快处理";
/// 连接器异常的事件编码
const CONNECTOR_ERROR_EVENT_CODE_DEFAULT: &str = "dbz.connector.error";
/// 连接器异常的事件名称（也是报警中心里消息模板的名称，全局唯一）
const CONNECTOR_ERROR_EVENT_NAME_DEFAULT: &str = "Debezium连接器异常";
/// 连接器异常的备注
const CONNECTOR_ERROR_REMARK_DEFAULT: &str =
    "连接器运行状态不是「运行中」时触发；上下游共用一个事件，按方向区分";
/// 指纹标签（逗号分隔的 labels 键名）
///
/// 上游与下游共用一个事件编码，两者一起才能定位到唯一一个连接器：只取
/// `connectorId` 的话，上游 3 号与下游 3 号（两张表各有自己的主键）会被并成
/// 同一条报警。
const CONNECTOR_ERROR_FINGER_PRINT_LABELS_DEFAULT: &str = "direction,connectorId";
/// 标题模板
const CONNECTOR_ERROR_TITLE_TEMPLATE_DEFAULT: &str =
    "Debezium {{directionName}}连接器<{{connectorName}}>异常";
/// 内容模板
///
/// 变量全部取自上报的 `labels`（见 `dbz_alarm_utils` 里的 `to_labels`）。
/// `relation` 是上游关系的短语：上游连接器填「无上游连接器」，下游填
/// 「上游连接器<xxx>」——模板没有条件判断，只能由发布方先拼好这一段。
const CONNECTOR_ERROR_CONTENT_TEMPLATE_DEFAULT: &str = "{{directionName}}连接器<{{connectorName}}>运行状态异常：\
{{runStatusName}}（{{runStatusValue}}）；\
Debezium 连接器 {{connectorNameOnCluster}}；\
{{relation}}；\
所属 Kafka Connect<{{kafkaConnectName}}>：{{kafkaConnectUrl}}";
/// 重复提醒间隔（毫秒）
const CONNECTOR_ERROR_REPEAT_INTERVAL_MS_DEFAULT: u64 = 30 * 60 * 1000;
/// 合并窗口（毫秒）
const CONNECTOR_ERROR_MERGE_WINDOW_MS_DEFAULT: u64 = 5 * 60 * 1000;
/// 报警消息队列编码的前缀
///
/// 报警中心按级别值种了 1~5 级的队列（`msg.publish.alarm.1` ~
/// `msg.publish.alarm.5`），级别与队列之间不存外键，靠这个前缀加级别值拼出
/// 主题。队列本身由报警中心的迁移脚本种好，dbz 只按编码查 ID 挂到事件策略上。
const ALARM_QUEUE_CODE_PREFIX: &str = "msg.publish.alarm";

fn alarm_event_source_code_default() -> String {
    ALARM_EVENT_SOURCE_CODE_DEFAULT.to_string()
}
fn alarm_event_source_name_default() -> String {
    ALARM_EVENT_SOURCE_NAME_DEFAULT.to_string()
}
fn alarm_event_source_remark_default() -> Option<String> {
    Some(ALARM_EVENT_SOURCE_REMARK_DEFAULT.to_string())
}
fn alarm_level_group_code_default() -> String {
    ALARM_LEVEL_GROUP_CODE_DEFAULT.to_string()
}
fn alarm_level_group_name_default() -> String {
    ALARM_LEVEL_GROUP_NAME_DEFAULT.to_string()
}
fn alarm_level_group_remark_default() -> Option<String> {
    Some(ALARM_LEVEL_GROUP_REMARK_DEFAULT.to_string())
}
fn alarm_level_code_default() -> String {
    ALARM_LEVEL_CODE_DEFAULT.to_string()
}
fn alarm_level_name_default() -> String {
    ALARM_LEVEL_NAME_DEFAULT.to_string()
}
fn alarm_level_value_default() -> i16 {
    ALARM_LEVEL_VALUE_DEFAULT
}
fn alarm_level_remark_default() -> Option<String> {
    Some(ALARM_LEVEL_REMARK_DEFAULT.to_string())
}
fn connector_error_event_code_default() -> String {
    CONNECTOR_ERROR_EVENT_CODE_DEFAULT.to_string()
}
fn connector_error_event_name_default() -> String {
    CONNECTOR_ERROR_EVENT_NAME_DEFAULT.to_string()
}
fn connector_error_finger_print_labels_default() -> Option<String> {
    Some(CONNECTOR_ERROR_FINGER_PRINT_LABELS_DEFAULT.to_string())
}
fn connector_error_title_template_default() -> Option<String> {
    Some(CONNECTOR_ERROR_TITLE_TEMPLATE_DEFAULT.to_string())
}
fn connector_error_content_template_default() -> Option<String> {
    Some(CONNECTOR_ERROR_CONTENT_TEMPLATE_DEFAULT.to_string())
}
fn connector_error_repeat_interval_ms_default() -> Option<u64> {
    Some(CONNECTOR_ERROR_REPEAT_INTERVAL_MS_DEFAULT)
}
fn connector_error_merge_window_ms_default() -> Option<u64> {
    Some(CONNECTOR_ERROR_MERGE_WINDOW_MS_DEFAULT)
}
fn connector_error_remark_default() -> Option<String> {
    Some(CONNECTOR_ERROR_REMARK_DEFAULT.to_string())
}
fn enabled_default() -> bool {
    true
}

/// # 报警消息队列编码
///
/// 报警级别与队列的约定：级别值 `1..=5` 对应 `msg.publish.alarm.{value}`。
/// 报警中心的迁移脚本按这个约定种了 5 条队列，dbz 与报警中心都按同一份规则
/// 拼编码，两边不需要在库里存外键。
///
/// ## 参数
///
/// - `value`：报警级别的值（1 最严重）。
///
/// ## 返回值
///
/// 发布队列编码，同时是消息中心的 NATS subject。
pub fn alarm_queue_code(value: i16) -> String {
    format!("{ALARM_QUEUE_CODE_PREFIX}.{value}")
}
