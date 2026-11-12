//! # 下游连接器配置：类识别与必填校验
//!
//! 下游连接器的 `connector_config` 是用户手写的 JSON，dbz 只负责把它下发给 Kafka
//! Connect。但有两类错误不会在下发时被 Connect 拦住，而是等到 task 启动才炸：
//!
//! * HTTP Sink 漏配 `http.url` —— 老版 `HttpSinkTask.start()` 直接对 null 建 URI，
//!   task 一起就 FAILED，然后被健康检查反复重启；
//! * URL 不是 http/https —— 请求阶段才报协议错误，被误判成「下游不可用」而重试。
//!
//! 这些在保存时就能判出来，没必要等到集群上炸一遍。校验只覆盖**确定键名**的
//! 两类 HTTP Sink；其它实现（`OtherHttpSink`）键名不统一，不做猜测。

use anyhow::bail;
use regex::Regex;
use serde_json::{Map, Value};
use std::sync::LazyLock;

/// URL 里的占位符，目前只允许 `${topic}`
static PLACEHOLDER: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\$\{([^}]*)}").expect("占位符正则是常量，不会编译失败"));

/// # 下游连接器的能力分类
///
/// 分类只用于「该注入什么配置、该校验什么必填项」，不代表运行时行为。同一个类名
/// 可能对应不同实现版本（`rebue.wheel.kafka.httpsink.HttpSinkConnector` 的 1.0.0
/// 前后行为不同），所以这里不做「是否支持 DLQ」的结论 —— 那需要插件版本，
/// 而版本只有 Connect 的 `/connector-plugins` 才知道。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorClass {
    /// rebue 自研 HTTP Sink（`rebue.*.HttpSinkConnector`）
    RebueHttpSink,
    /// Confluent HTTP Sink（`io.confluent.connect.http.HttpSinkConnector`）
    ConfluentHttpSink,
    /// 其它 HTTP Sink（类名带 HttpSinkConnector，但不是上面两家）
    OtherHttpSink,
    /// JDBC Sink
    JdbcSink,
    /// 认不出来
    Unknown,
}

impl ConnectorClass {
    /// # 从连接器配置识别类
    ///
    /// 只看 `connector.class`，大小写不敏感：用户手工填类名时大小写经常不一致。
    pub fn detect(config: &Map<String, Value>) -> Self {
        let class = text(config, "connector.class")
            .unwrap_or_default()
            .to_ascii_lowercase();
        if class.contains("httpsinkconnector") || class.contains("connect.http") {
            if class.contains("rebue") {
                Self::RebueHttpSink
            } else if class.contains("confluent") {
                Self::ConfluentHttpSink
            } else {
                Self::OtherHttpSink
            }
        } else if class.contains("jdbcsinkconnector") {
            Self::JdbcSink
        } else {
            Self::Unknown
        }
    }

    /// 是否 HTTP Sink（三类 HTTP 实现都算）
    pub fn is_http_sink(self) -> bool {
        matches!(
            self,
            Self::RebueHttpSink | Self::ConfluentHttpSink | Self::OtherHttpSink
        )
    }

    /// 键名是否可预期：只有这一类才值得做必填校验
    pub fn has_known_http_keys(self) -> bool {
        matches!(self, Self::RebueHttpSink | Self::ConfluentHttpSink)
    }

    /// # 这一类的请求级重试参数键名
    ///
    /// 两家实现的键名不同，而且都**只认自己的**：写错了不会报错，只会变成一条
    /// unused 告警，页面上的重试配置看上去生效、实际没人读。
    ///
    /// * rebue 2.x：`http.retry.max` / `http.retry.backoff.ms`
    /// * Confluent：`max.retries` / `retry.backoff.ms`
    ///
    /// 认不出的实现返回 `None`：宁可不写，也不写一份假配置。
    pub fn retry_keys(self) -> Option<(&'static str, &'static str)> {
        match self {
            Self::RebueHttpSink => Some(("http.retry.max", "http.retry.backoff.ms")),
            Self::ConfluentHttpSink => Some(("max.retries", "retry.backoff.ms")),
            Self::OtherHttpSink | Self::JdbcSink | Self::Unknown => None,
        }
    }
}

/// # 校验下游连接器配置里的必填项
///
/// 只校验 HTTP Sink 的地址：`http.url`（rebue 与本项目新版）或 `http.api.url`
/// （Confluent，以及 rebue 老配置）。其余键交给 Connect 的 `validate` 与插件自己。
///
/// ## 返回值
/// * `Ok(())` - 配置可用，或不是需要校验的类
/// * `Err` - HTTP Sink 缺地址、协议不对或多写了不支持的占位符
pub fn validate_sink_config(config: &Map<String, Value>) -> anyhow::Result<()> {
    let class = ConnectorClass::detect(config);
    if !class.has_known_http_keys() {
        return Ok(());
    }

    let url = text(config, "http.url").or_else(|| text(config, "http.api.url"));
    let Some(url) = url else {
        bail!(
            "HTTP Sink 必须配置 http.url（Confluent 用 http.api.url）：缺失时 task 一启动就失败，\
             健康检查会把它反复重启"
        );
    };

    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| anyhow::anyhow!("http.url<{url}>必须以 http:// 或 https:// 开头"))?;
    if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
        bail!("http.url<{url}>只支持 http / https 协议");
    }
    if rest.split('/').next().unwrap_or_default().is_empty() {
        bail!("http.url<{url}>缺少主机名");
    }

    for capture in PLACEHOLDER.captures_iter(url) {
        let name = capture.get(1).map(|one| one.as_str()).unwrap_or_default();
        if name != "topic" {
            bail!("http.url<{url}>里的占位符 ${{{name}}} 不受支持，目前只支持 ${{topic}}");
        }
    }

    Ok(())
}

/// 取一个非空字符串配置项（去首尾空白）
fn text<'a>(config: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    config
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn object(value: Value) -> Map<String, Value> {
        value.as_object().cloned().unwrap_or_default()
    }

    #[test]
    fn detects_http_sink_vendors() {
        assert_eq!(
            ConnectorClass::detect(&object(
                json!({"connector.class": "rebue.wheel.kafka.httpsink.HttpSinkConnector"})
            )),
            ConnectorClass::RebueHttpSink
        );
        assert_eq!(
            ConnectorClass::detect(&object(
                json!({"connector.class": "rebue.kafka.httpsink.HttpSinkConnector"})
            )),
            ConnectorClass::RebueHttpSink
        );
        assert_eq!(
            ConnectorClass::detect(&object(
                json!({"connector.class": "io.confluent.connect.http.HttpSinkConnector"})
            )),
            ConnectorClass::ConfluentHttpSink
        );
        assert_eq!(
            ConnectorClass::detect(&object(
                json!({"connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector"})
            )),
            ConnectorClass::JdbcSink
        );
        assert_eq!(
            ConnectorClass::detect(&object(json!({"connector.class": "com.acme.Sink"}))),
            ConnectorClass::Unknown
        );
    }

    /// 老实现的 `HttpSinkTask.start()` 会对 null 建 URI，task 必挂；这种配置要在保存时就拦住
    #[test]
    fn rejects_http_sink_without_url() {
        let config = object(json!({
            "connector.class": "rebue.wheel.kafka.httpsink.HttpSinkConnector",
            "topics": "dbz.orders"
        }));
        assert!(validate_sink_config(&config).is_err());
    }

    #[test]
    fn accepts_legacy_api_url_and_topic_placeholder() {
        let config = object(json!({
            "connector.class": "io.confluent.connect.http.HttpSinkConnector",
            "http.api.url": "http://magic/sync/${topic}"
        }));
        assert!(validate_sink_config(&config).is_ok());
    }

    #[test]
    fn rejects_bad_scheme_host_and_placeholder() {
        for url in ["ftp://host/x", "http:///x", "http://host/${key}", "host/x"] {
            let config = object(json!({
                "connector.class": "rebue.kafka.httpsink.HttpSinkConnector",
                "http.url": url
            }));
            assert!(validate_sink_config(&config).is_err(), "应拒绝 {url}");
        }
    }

    /// 不认识的类不猜键名，避免误拒合法的第三方实现
    #[test]
    fn unknown_class_is_not_validated() {
        let config = object(json!({"connector.class": "com.acme.CustomSink"}));
        assert!(validate_sink_config(&config).is_ok());
    }
}
