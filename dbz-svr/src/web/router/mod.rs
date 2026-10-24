mod dbz_kafka_connect_router;
mod dbz_kafka_connector_router;
mod dbz_sink_connector_router;
mod dbz_source_connector_router;
mod dbz_source_connector_snapshot_router;

/// # 路由自检
///
/// 每个 `#[router]` 各自装配一棵 `Router`，最终由框架按 `ROUTER_SLICE` 逐个 `.merge()`
/// 进来。合并时路径形状撞车会**直接 panic**，而这条路只有起服务才会走到 —— 放在这里
/// 让 `cargo test` 提前发现，尤其是「`/dbz/source-connector/{id}` 与快照的
/// `/dbz/source-connector/snapshot` 能否共存」这种依赖 matchit 版本的事。
///
/// 写在 lib 内而不是 `tests/`：集成测试里 `ROUTER_SLICE` 是空的（lib 的目标文件没被
/// 引用，链接器把条目丢了），只有同一编译单元才拿得到真实的路由表。
#[cfg(test)]
mod tests {
    use axum::Router;

    #[test]
    fn routers_merge_without_conflict() {
        let mut router = Router::new();
        for build_router in robotech::web::ROUTER_SLICE.iter() {
            router = router.merge(build_router());
        }
        // 合并成功即说明没有冲突；这里不发请求，只验证装配阶段不 panic
        drop(router);
    }
}
