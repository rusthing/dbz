//! # 上游连接器再同步路由
//!
//! 两个手写路由：`GET /dbz/source-connector/snapshot/preview` 与
//! `POST /dbz/source-connector/snapshot`。
//!
//! `#[router]` 宏按**结构体名**推导 ctrl 模块
//! （`DbzSourceConnectorSnapshotRouter` → `dbz_source_connector_snapshot_ctrl`），
//! 结构体名不能省掉 `DbzSourceConnector` 前缀。
//!
//! 生成的代码会一次性导入 axum 的四个路由构造器
//! （`delete` / `get` / `post` / `put`），本模块只用到 `get` 与 `post`，
//! 因此在模块级屏蔽未使用导入的告警。
#![allow(unused_imports)]

use robotech::macros::router;

#[router(routes [
    ("/dbz/source-connector/snapshot/preview", get(preview_snapshot)),
    ("/dbz/source-connector/snapshot", post(execute_snapshot)),
])]
struct DbzSourceConnectorSnapshotRouter;
