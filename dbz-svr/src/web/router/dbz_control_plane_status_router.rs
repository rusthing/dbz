//! # 控制面状态路由

#![allow(unused_imports)]

use robotech::macros::router;

#[router(routes [
    ("/dbz/control-plane/status/refresh", get(refresh)),
    ("/dbz/control-plane/connector/reset-offsets", post(reset_offsets)),
])]
struct DbzControlPlaneStatusRouter;
