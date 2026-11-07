//! # Kafka Connect 健康检查路由
//!
//! `GET /dbz/kafka-connect/health`。静态段 `health` 与 CRUD 的
//! `/dbz/kafka-connect/{id}` 只在同一层重叠，matchit 里静态段优先；`web/router`
//! 下的路由自检单测会在装配阶段就把冲突炸出来。
//!
//! 生成的代码一次性导入四个路由构造器，本模块只用到 `get`，因此屏蔽未使用导入告警。

#![allow(unused_imports)]

use robotech::macros::router;

#[router(routes [("/dbz/kafka-connect/health", get(health))])]
struct DbzKafkaConnectHealthRouter;
