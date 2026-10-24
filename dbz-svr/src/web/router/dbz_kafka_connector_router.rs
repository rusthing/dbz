//! # Kafka 连接器路由
//!
//! 注册 Kafka Connect 连接器运维接口的自定义路由。
//!
//! 路径是 `/dbz/kafka-connector/*`：服务前缀 + 资源名的写法与其他接口一致
//! （集群是 `/dbz/kafka-connect`、上下游是 `/dbz/source-connector`）。
//! 这里没走 `#[router(crud)]`，路径是逐个手写的，所以不受结构体名影响。
//!
//! `#[router]` 宏按**结构体名**推导 ctrl 模块：`DbzKafkaConnectorRouter` →
//! `crate::web::ctrl::dbz_kafka_connector_ctrl`。
//!
//! `#[router]` 宏生成的代码会一次性导入 axum 的四个路由构造器
//! （`delete` / `get` / `post` / `put`），本模块四个都用到了，
//! 仍然保留模块级的未使用导入豁免（宏展开的导入列表是固定的）。
#![allow(unused_imports)]

use robotech::macros::router;

#[router(routes [
    ("/dbz/kafka-connector/plugins", get(list_connector_plugins)),
    ("/dbz/kafka-connector/list", get(list_connectors)),
    ("/dbz/kafka-connector/restart", post(restart_connector)),
    ("/dbz/kafka-connector/task/restart", post(restart_connector_task)),
    ("/dbz/kafka-connector/stop", put(stop_connector)),
    ("/dbz/kafka-connector/resume", put(resume_connector)),
    ("/dbz/kafka-connector/offsets", delete(reset_connector_offsets)),
    ("/dbz/kafka-connector", delete(delete_connector)),
])]
struct DbzKafkaConnectorRouter;
