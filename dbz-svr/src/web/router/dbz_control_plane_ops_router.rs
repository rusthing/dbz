//! # 控制面操作路由

#![allow(unused_imports)]

use robotech::macros::router;

#[router(routes [
    ("/dbz/control-plane/edges", get(list_edges)),
    ("/dbz/control-plane/edges", put(upsert_edge)),
    ("/dbz/control-plane/edges", delete(remove_edge)),
    ("/dbz/control-plane/node-delete", post(delete_node)),
])]
struct DbzControlPlaneOpsRouter;
