//! # 连接器路由
//!
//! 注册 Kafka Connect 连接器运维接口的自定义路由。
//!
//! `#[router]` 宏生成的代码会一次性导入 axum 的四个路由构造器
//! （`delete` / `get` / `post` / `put`），本模块只用到 `get` 与 `post`，
//! 因此在模块级屏蔽未使用导入的告警。
#![allow(unused_imports)]

use robotech::macros::router;

#[router(routes [
    ("/dbz/connector/plugins", get(list_connector_plugins)),
    ("/dbz/connector/list", get(list_connectors)),
    ("/dbz/connector/restart", post(restart_connector)),
    ("/dbz/connector/task/restart", post(restart_connector_task)),
])]
struct DbzConnectorRouter;
