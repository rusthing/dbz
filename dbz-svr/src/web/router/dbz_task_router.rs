use robotech::macros::router;

#[router(crud, routes[
    ("/dbz/task/register", post(register)),
    ("/dbz/task/unregister/{id}", post(unregister)),
    ("/dbz/task/check-status/{id}", post(check_status)),
    ("/dbz/task/restart/{id}", post(restart)),
])]
struct DbzTaskRouter;