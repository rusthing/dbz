//! # 错误文本整理
//!
//! 健康检查把「这次为什么失败」写进 `run_msg`（`varchar(500)`）。来源有两类：
//!
//! - **anyhow 的错误链**（连接超时、HTTP 4xx、部署被拒）：`{:#}` 已经是单行，
//!   但可能很长，也可能夹着换行；
//! - **Debezium 的 Java 异常栈**（连接器或任务 trace）：几十行，真正有用的是首行
//!   那句异常，比如 `Access denied for user 'dbz'`。
//!
//! 两者最后都要落进同一个列，所以在这里统一成「一行、去多余空白、按字符截断」。
//! 截断按字符边界切，不按字节，否则中文会被切坏。

/// `run_msg` 列宽（`varchar(500)`）。写库前一律按这个长度截断。
pub const RUN_MSG_MAX: usize = 500;

/// # 整理成一行并截断
///
/// 连续空白（含换行、制表）折成一个空格，再按字符数截断。截断就是直接切掉，
/// 不补省略号：这类文案是给运维看的原始错误，多一个符号反而容易被当成原文的一部分。
///
/// ## 参数
/// * `msg` - 原始文本
/// * `max` - 最大字符数
///
/// ## 返回值
/// 整理后的文本；原文全为空白时返回空串
pub fn one_line(msg: &str, max: usize) -> String {
    let mut out = String::with_capacity(msg.len().min(max));
    let mut pending_space = false;
    let mut used = 0;
    for ch in msg.chars() {
        if ch.is_whitespace() {
            pending_space = !out.is_empty();
            continue;
        }
        // 空格也算一个字符，否则「全是空格 + 长词」的文本会超出列宽
        if pending_space {
            if used + 1 >= max {
                break;
            }
            out.push(' ');
            used += 1;
            pending_space = false;
        }
        if used >= max {
            break;
        }
        out.push(ch);
        used += 1;
    }
    out
}

/// # 取异常栈的首行
///
/// Debezium 的 trace 是整段 Java 栈：首行是异常本身，其余是 `at ...` 调用链。
/// 没有内容时返回 `None`（有些集群只在连接器级给 trace，任务级为空）。
///
/// ## 参数
/// * `trace` - 原始 trace 文本
/// * `max` - 首行最大字符数
pub fn trace_head(trace: &str, max: usize) -> Option<String> {
    let head = trace.lines().map(str::trim).find(|line| !line.is_empty())?;
    let head = one_line(head, max);
    if head.is_empty() { None } else { Some(head) }
}

#[cfg(test)]
mod tests {
    use super::{one_line, trace_head};

    #[test]
    fn one_line_folds_whitespace() {
        assert_eq!(one_line("a\n\n b\tc  ", 100), "a b c");
        assert_eq!(one_line("   ", 100), "");
    }

    #[test]
    fn one_line_truncates_by_chars() {
        // 按字符截断：4 个汉字不会因为占 12 字节而被切成 1 个
        assert_eq!(one_line("连接失败很严重", 4), "连接失败");
        assert_eq!(one_line("abcdef", 3), "abc");
    }

    #[test]
    fn trace_head_takes_first_non_empty_line() {
        let trace =
            "\n io.debezium.DebeziumException: Access denied\n\tat io.debezium.Foo.bar(Foo.java:1)";
        assert_eq!(
            trace_head(trace, 100).as_deref(),
            Some("io.debezium.DebeziumException: Access denied")
        );
        assert_eq!(trace_head("   \n\t", 100), None);
    }
}
