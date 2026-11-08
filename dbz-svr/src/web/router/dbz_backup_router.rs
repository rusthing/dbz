//! # Kafka 集群备份 / 恢复路由
//!
//! `GET /dbz/backup/export`、`POST /dbz/backup/restore`。
//!
//! 生成的代码一次性导入四个路由构造器，本模块只用到 `get` / `post`，因此屏蔽未使用
//! 导入告警。

#![allow(unused_imports)]

use robotech::macros::router;

#[router(routes [
    ("/dbz/backup/export", get(export)),
    ("/dbz/backup/restore", post(restore))
])]
struct DbzBackupRouter;
