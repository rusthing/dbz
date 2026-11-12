//! # Kafka Connect 死信队列（DLQ）工具
//!
//! 死信的唯一事实来源是 Kafka Connect 写进 DLQ topic 的记录。本模块只做两件事：
//!
//! 1. **下发配置**：部署下游连接器前，把 `errors.tolerance=all`、
//!    `errors.deadletterqueue.*`、`errors.retry.*`、`errors.log.*` 合并进
//!    `connector_config`。坏记录的判定、重试、原始报文保全、offset 提交全部由
//!    Connect 完成，dbz 不再自己判定「哪条消息失败了」。
//! 2. **解读记录**：把 DLQ 记录的 `__connect.errors.*` 上下文头解析成页面能展示的
//!    字段，并按异常类名做粗粒度分类。
//!
//! ## 为什么不再让业务脚本上报
//!
//! 旧实现里，magic 脚本 HTTP 200 + `{"result":-2}` 时 Connect 认为成功并提交偏移，
//! 于是要求脚本主动调 dbz 的 `/report` 写死信。这等于把「坏记录有没有被记下来」
//! 交给被监控方自觉：脚本漏报、进程崩、网络断，数据就静默丢了，而且 Kafka 里
//! 连原始报文都没有。正确做法是让失败在 Connect 层可见（业务失败必须返回非 2xx），
//! 由框架把原始记录连同错误上下文写进 DLQ topic。
//!
//! ## 覆盖的边界
//!
//! Connect 的 DLQ 只适用于 **sink** 连接器；source 连接器没有「把外部记录转成
//! Kafka 记录」这一步，框架不提供 DLQ。另外 `errors.tolerance=all` 会跳过错误并
//! 继续消费，所以必须同时打开 DLQ 与 context headers，否则同样等于丢数据。

use crate::config::DeadLetterConfig;
use crate::dic::{DeadLetterFailureCategory, DeadLetterFailureSubtype};
use crate::utils::dbz_sink_config_utils::ConnectorClass;
use anyhow::{anyhow, bail};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;

/// `errors.tolerance`：必须 `all`，否则一条坏记录就会让 task FAILED
pub const KEY_ERRORS_TOLERANCE: &str = "errors.tolerance";
/// DLQ topic 名
pub const KEY_DLQ_TOPIC: &str = "errors.deadletterqueue.topic.name";
/// DLQ topic 副本数
pub const KEY_DLQ_REPLICATION_FACTOR: &str = "errors.deadletterqueue.topic.replication.factor";
/// 是否把原始 topic/partition/offset、异常类与堆栈写进 DLQ 记录头
pub const KEY_DLQ_CONTEXT_HEADERS: &str = "errors.deadletterqueue.context.headers.enable";
/// 框架层重试总时长
pub const KEY_ERRORS_RETRY_TIMEOUT: &str = "errors.retry.timeout";
/// 框架层重试最大退避
pub const KEY_ERRORS_RETRY_DELAY_MAX: &str = "errors.retry.delay.max.ms";
/// 是否写 Connect 日志
pub const KEY_ERRORS_LOG_ENABLE: &str = "errors.log.enable";
/// 日志是否包含原始报文
pub const KEY_ERRORS_LOG_INCLUDE_MESSAGES: &str = "errors.log.include.messages";

/// DLQ 记录头前缀。Apache Kafka 用双下划线，Confluent 文档里出现过单下划线，
/// 读取时两种都认。
pub const DLQ_HEADER_PREFIXES: [&str; 2] = ["__connect.errors.", "_connect.errors."];

/// 源 topic / 分区 / offset / 异常类名 / 异常消息 / 堆栈 / 连接器名
pub const HDR_TOPIC: &str = "topic";
pub const HDR_PARTITION: &str = "partition";
pub const HDR_OFFSET: &str = "offset";
pub const HDR_EXCEPTION_CLASS: &str = "exception.class.name";
pub const HDR_EXCEPTION_MESSAGE: &str = "exception.message";
pub const HDR_EXCEPTION_STACKTRACE: &str = "exception.stacktrace";
pub const HDR_CONNECTOR_NAME: &str = "connector.name";

/// # 把 DLQ 配置合并进下游连接器配置
///
/// 这些 key 由控制面统一决定并**强制覆盖**：只要有一条连接器没开 DLQ，
/// 页面就会少一类坏记录，退回到「静默丢数据」。用户手写的其它 key 一个不丢。
/// HTTP Sink 额外补 `max.retries` / `retry.backoff.ms`，把请求级重试也交给 Connect。
///
/// ## 返回值
/// * `Ok(String)` - 补齐后的配置（pretty JSON）
/// * `Err` - 配置不是合法 JSON 对象，或 DLQ topic 名不合法
pub fn ensure_sink_dlq_config(
    connector_config: &str,
    cfg: &DeadLetterConfig,
) -> anyhow::Result<String> {
    let topic = cfg.topic.trim();
    validate_topic_name(topic)?;

    let mut object: Map<String, Value> = serde_json::from_str(connector_config)
        .map_err(|e| anyhow!("连接器配置不是合法的 JSON 对象: {e}"))?;
    reject_dlq_in_subscriptions(&object, topic)?;

    object.insert(KEY_ERRORS_TOLERANCE.into(), json!("all"));
    object.insert(KEY_DLQ_TOPIC.into(), json!(topic));
    object.insert(
        KEY_DLQ_REPLICATION_FACTOR.into(),
        json!(cfg.replication_factor.to_string()),
    );
    object.insert(KEY_DLQ_CONTEXT_HEADERS.into(), json!("true"));
    object.insert(
        KEY_ERRORS_RETRY_TIMEOUT.into(),
        json!(cfg.retry_timeout_ms.to_string()),
    );
    object.insert(
        KEY_ERRORS_RETRY_DELAY_MAX.into(),
        json!(cfg.retry_delay_max_ms.to_string()),
    );
    object.insert(
        KEY_ERRORS_LOG_ENABLE.into(),
        json!(cfg.log_enable.to_string()),
    );
    object.insert(
        KEY_ERRORS_LOG_INCLUDE_MESSAGES.into(),
        json!(cfg.log_include_messages.to_string()),
    );

    if ConnectorClass::detect(&object).is_http_sink() {
        object.insert(
            "max.retries".into(),
            json!(cfg.http_max_retries.to_string()),
        );
        object.insert(
            "retry.backoff.ms".into(),
            json!(cfg.http_retry_backoff_ms.to_string()),
        );
    }

    serde_json::to_string_pretty(&object).map_err(|e| anyhow!("序列化连接器配置失败: {e}"))
}

/// # 生效中的 DLQ topic 名
pub fn dlq_topic(cfg: &DeadLetterConfig) -> &str {
    cfg.topic.trim()
}

/// Kafka topic 名只允许字母数字与 `.` `_` `-`，避免把非法字符下发到 Connect
fn validate_topic_name(topic: &str) -> anyhow::Result<()> {
    if topic.is_empty() || topic.len() > 249 {
        bail!("DLQ topic 名不能为空且不能超过 249 个字符");
    }
    if !topic
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-')
    {
        bail!("DLQ topic 名<{topic}>只能包含字母、数字、点、下划线和连字符");
    }
    Ok(())
}

/// DLQ topic 不能被连接器自己的订阅范围圈进去，否则 Connect 会拒绝下发配置
/// （即便下发成功，也会把死信再当业务消息消费一次）。
///
/// `topics` 是逗号分隔的精确列表，直接比对；`topics.regex` 是 Java 正则，Rust 的
/// `regex` 语法与之不完全相同，因此这里只在**能编译且确实命中**时拦下，编译不了就
/// 交给 Connect 自己判断，避免用 Rust 方言误拒合法配置。
fn reject_dlq_in_subscriptions(config: &Map<String, Value>, topic: &str) -> anyhow::Result<()> {
    if let Some(topics) = config.get("topics").and_then(Value::as_str)
        && topics
            .split(',')
            .map(str::trim)
            .any(|one| !one.is_empty() && one == topic)
    {
        bail!(
            "连接器的 topics<{topics}>包含 DLQ topic<{topic}>，会造成循环消费，请修改 DLQ topic 名"
        );
    }
    if let Some(pattern) = config
        .get("topics.regex")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        && let Ok(re) = regex::Regex::new(pattern)
        && re.is_match(topic)
    {
        bail!("连接器的 topics.regex<{pattern}>会匹配到 DLQ topic<{topic}>，会造成循环消费");
    }
    Ok(())
}

/// # 读取一个 DLQ 上下文头
///
/// 头值是字节串，按 UTF-8 宽松解码；两种前缀都试一遍。
pub fn header_text(headers: &BTreeMap<String, Vec<u8>>, name: &str) -> Option<String> {
    for prefix in DLQ_HEADER_PREFIXES {
        let key = format!("{prefix}{name}");
        if let Some(bytes) = headers.get(&key) {
            return Some(String::from_utf8_lossy(bytes).into_owned());
        }
    }
    None
}

/// # 从异常类名与消息推导展示用的分类
///
/// 这是**展示分类**，不参与任何重试决策：重试由 Connect 决定，dbz 不再按分类
/// 分流。分类只影响页面一句话说明与「是否允许改报文后重投」。
pub fn classify_error(
    class_name: &str,
    message: &str,
) -> (DeadLetterFailureCategory, DeadLetterFailureSubtype) {
    let lower = class_name.to_ascii_lowercase();
    let deterministic = [
        "dataexception",
        "serializationexception",
        "deserializationexception",
        "jsonparse",
        "numberformat",
        "illegalargument",
        "constraintviolation",
        "schemabuilder",
        "configexception",
    ]
    .iter()
    .any(|marker| lower.contains(marker));
    let transient = [
        "retriable",
        "timeout",
        "ioexception",
        "connectexception",
        "http",
        "network",
        "unknownhost",
    ]
    .iter()
    .any(|marker| lower.contains(marker));

    let category = if deterministic {
        DeadLetterFailureCategory::Deterministic
    } else if transient {
        DeadLetterFailureCategory::Transient
    } else {
        DeadLetterFailureCategory::Unknown
    };

    let text = message.to_ascii_lowercase();
    let field_validation = [
        "required field",
        "missing field",
        "invalid value",
        "field validation",
        "not nullable",
        "must not be null",
        "unknown field",
    ]
    .iter()
    .any(|marker| text.contains(marker));
    let subtype = if field_validation {
        DeadLetterFailureSubtype::FieldValidation
    } else {
        DeadLetterFailureSubtype::Other
    };
    (category, subtype)
}

/// # 把 DLQ 记录体转成可存 JSONB 的值
///
/// Connect 写进 DLQ 的是**原始字节**：多数场景是 JSON 文本，直接解析；不是 JSON
/// 或不是 UTF-8 时包一层 `{"__raw_utf8": ...}`，重投时再原样取回，不静默丢弃。
pub fn payload_value(bytes: &[u8]) -> Value {
    match std::str::from_utf8(bytes) {
        Ok(text) => {
            serde_json::from_str::<Value>(text).unwrap_or_else(|_| json!({ "__raw_utf8": text }))
        }
        Err(_) => json!({ "__raw_utf8": String::from_utf8_lossy(bytes) }),
    }
}

/// # 把存下来的报文还原成投递字节
///
/// 与 [`payload_value`] 对称：普通 JSON 重新序列化，包装过的原文直接取回。
pub fn payload_bytes(payload: &Value) -> Vec<u8> {
    if let Some(raw) = payload.get("__raw_utf8").and_then(Value::as_str) {
        return raw.as_bytes().to_vec();
    }
    serde_json::to_vec(payload).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> DeadLetterConfig {
        DeadLetterConfig::default()
    }

    #[test]
    fn injects_standard_dlq_keys_and_keeps_user_keys() {
        let source = r#"{"connector.class":"io.confluent.connect.http.HttpSinkConnector","topics":"dbz.orders","http.api.url":"http://magic/sync","tasks.max":"2"}"#;
        let result = ensure_sink_dlq_config(source, &config()).unwrap();
        let value: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(value[KEY_ERRORS_TOLERANCE], "all");
        assert_eq!(value[KEY_DLQ_TOPIC], "dbz-connect-dlq");
        assert_eq!(value[KEY_DLQ_CONTEXT_HEADERS], "true");
        assert_eq!(value["http.api.url"], "http://magic/sync");
        assert_eq!(value["tasks.max"], "2");
        assert_eq!(value["max.retries"], "10");
        assert_eq!(value["retry.backoff.ms"], "3000");
    }

    #[test]
    fn overrides_user_supplied_tolerance_and_topic() {
        let source = r#"{"connector.class":"io.confluent.connect.jdbc.JdbcSinkConnector","errors.tolerance":"none","errors.deadletterqueue.topic.name":"other-dlq"}"#;
        let result = ensure_sink_dlq_config(source, &config()).unwrap();
        let value: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(value[KEY_ERRORS_TOLERANCE], "all");
        assert_eq!(value[KEY_DLQ_TOPIC], "dbz-connect-dlq");
        // 非 HTTP Sink 不写 HTTP Sink 专有的重试参数
        assert!(value.get("max.retries").is_none());
    }

    #[test]
    fn rejects_invalid_json_and_topic_name() {
        assert!(ensure_sink_dlq_config("not-json", &config()).is_err());
        let mut cfg = config();
        cfg.topic = "bad topic!".into();
        assert!(ensure_sink_dlq_config("{}", &cfg).is_err());
    }

    #[test]
    fn dlq_topic_must_not_be_in_subscriptions() {
        let list = r#"{"topics":"dbz.orders, dbz-connect-dlq"}"#;
        assert!(ensure_sink_dlq_config(list, &config()).is_err());
        let regex = r#"{"topics.regex":"dbz-.*"}"#;
        assert!(ensure_sink_dlq_config(regex, &config()).is_err());
        let safe = r#"{"topics":"dbz.orders","topics.regex":"dbz\\.orders\\..*"}"#;
        assert!(ensure_sink_dlq_config(safe, &config()).is_ok());
        // Java 支持后行断言、Rust regex 不支持：编译不了就交给 Connect 判断，不误拒
        let java_only = r#"{"topics.regex":"(?<=dbz).*"}"#;
        assert!(ensure_sink_dlq_config(java_only, &config()).is_ok());
    }

    #[test]
    fn header_prefixes_are_both_accepted() {
        let mut headers = BTreeMap::new();
        headers.insert("__connect.errors.topic".into(), b"dbz.orders".to_vec());
        headers.insert(
            "_connect.errors.exception.class.name".into(),
            b"org.apache.kafka.connect.errors.DataException".to_vec(),
        );
        assert_eq!(
            header_text(&headers, HDR_TOPIC).as_deref(),
            Some("dbz.orders")
        );
        assert_eq!(
            header_text(&headers, HDR_EXCEPTION_CLASS).as_deref(),
            Some("org.apache.kafka.connect.errors.DataException")
        );
    }

    /// Apache Kafka `DeadLetterQueueReporter` 里 `HEADER_PREFIX + <suffix>` 的实际常量，
    /// 这里按真实 key 锁一遍，避免常量改动后线上同步静默拿不到源坐标/连接器名。
    #[test]
    fn header_constants_match_connect_dlq_reporter_keys() {
        let mut headers = BTreeMap::new();
        let pairs = [
            ("topic", "dbz.orders"),
            ("partition", "3"),
            ("offset", "42"),
            ("connector.name", "http-sink-orders"),
            (
                "exception.class.name",
                "org.apache.kafka.connect.errors.DataException",
            ),
            ("exception.message", "missing field id"),
            ("exception.stacktrace", "at ..."),
        ];
        for (suffix, value) in pairs {
            headers.insert(
                format!("__connect.errors.{suffix}"),
                value.as_bytes().to_vec(),
            );
        }
        assert_eq!(
            header_text(&headers, HDR_TOPIC).as_deref(),
            Some("dbz.orders")
        );
        assert_eq!(header_text(&headers, HDR_PARTITION).as_deref(), Some("3"));
        assert_eq!(header_text(&headers, HDR_OFFSET).as_deref(), Some("42"));
        assert_eq!(
            header_text(&headers, HDR_CONNECTOR_NAME).as_deref(),
            Some("http-sink-orders")
        );
        assert_eq!(
            header_text(&headers, HDR_EXCEPTION_CLASS).as_deref(),
            Some("org.apache.kafka.connect.errors.DataException")
        );
        assert_eq!(
            header_text(&headers, HDR_EXCEPTION_MESSAGE).as_deref(),
            Some("missing field id")
        );
        assert_eq!(
            header_text(&headers, HDR_EXCEPTION_STACKTRACE).as_deref(),
            Some("at ...")
        );
    }

    #[test]
    fn classification_is_display_only_but_stable() {
        assert_eq!(
            classify_error(
                "org.apache.kafka.connect.errors.DataException",
                "missing field id"
            )
            .0,
            DeadLetterFailureCategory::Deterministic
        );
        assert_eq!(
            classify_error(
                "org.apache.kafka.connect.errors.RetriableException",
                "timeout"
            )
            .0,
            DeadLetterFailureCategory::Transient
        );
        assert_eq!(
            classify_error("java.lang.RuntimeException", "oops").0,
            DeadLetterFailureCategory::Unknown
        );
        assert_eq!(
            classify_error(
                "org.apache.kafka.connect.errors.DataException",
                "missing field id"
            )
            .1,
            DeadLetterFailureSubtype::FieldValidation
        );
    }

    #[test]
    fn non_json_payload_is_kept_as_raw_text() {
        let bytes = b"id,order_no\n1001,SO-1".to_vec();
        let value = payload_value(&bytes);
        assert_eq!(
            value.get("__raw_utf8").and_then(Value::as_str),
            Some("id,order_no\n1001,SO-1")
        );
        assert_eq!(payload_bytes(&value), bytes);
    }

    #[test]
    fn non_utf8_payload_does_not_panic_or_disappear() {
        let value = payload_value(&[0xff, 0xfe, 0x00]);
        assert!(value.get("__raw_utf8").is_some());
        assert!(!payload_bytes(&value).is_empty());
    }

    #[test]
    fn json_payload_round_trips() {
        let bytes = br#"{"id":1001,"order_no":"SO-1"}"#.to_vec();
        let value = payload_value(&bytes);
        assert_eq!(value["id"], 1001);
        let restored = payload_bytes(&value);
        let parsed: Value = serde_json::from_slice(&restored).unwrap();
        assert_eq!(parsed["order_no"], "SO-1");
    }
}
