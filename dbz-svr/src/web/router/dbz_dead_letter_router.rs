//! # 死信路由
//!
//! 全部是手写接口（上报 / 分页 / 计数 / 重投 / 废弃），没有 CRUD，因此单独一棵
//! `#[router(routes ...)]` 路由树；生成的代码一次性导入四个构造器，这里只用到
//! `get` / `post`，屏蔽未使用导入告警。

#![allow(unused_imports)]

use robotech::macros::router;

#[router(routes [
    ("/dbz/dead-letter/report", post(report)),
    ("/dbz/dead-letter/page", get(page)),
    ("/dbz/dead-letter/pending-count", get(pending_count)),
    ("/dbz/dead-letter/retry", post(retry)),
    ("/dbz/dead-letter/edit-payload", post(edit_payload)),
    ("/dbz/dead-letter/discard", post(discard)),
    ("/dbz/dead-letter/mark-handled", post(mark_handled)),
    ("/dbz/dead-letter/unmark-handled", post(unmark_handled)),
])]
struct DbzDeadLetterRouter;
