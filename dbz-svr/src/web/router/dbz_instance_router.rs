use robotech::macros::router;

#[router(crud, routes[
    ("/dbz/instance/register", post(register)),           // 注册实例
    ("/dbz/instance/unregister/{id}", post(unregister)),  // 注销实例
    ("/dbz/instance/check-status/{id}", post(check_status)), // 查询连接器状态
    ("/dbz/instance/restart/{id}", post(restart)),        // 重启连接器
])]
struct DbzInstanceRouter;
