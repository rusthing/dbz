//! # 控制面画布布局路由

#![allow(unused_imports)]

use robotech::macros::router;

#[router(routes [
    ("/dbz/control-plane/layout", get(list)),
    ("/dbz/control-plane/layout", put(upsert)),
    ("/dbz/control-plane/layout", delete(remove)),
])]
struct DbzControlPlaneLayoutRouter;
