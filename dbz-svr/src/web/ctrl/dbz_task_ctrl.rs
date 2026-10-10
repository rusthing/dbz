use robotech::macros::ctrl;

#[ctrl]
struct DbzTaskCtrl;

/// # 注册 Debezium 连接器
///
/// 创建数据库记录，向 Kafka Connect 注册连接器，并创建定时检查的任务计划。
#[utoipa::path(
    post,
    path = "/dbz/task/register",
    request_body = DbzTaskAddDto,
    responses((status = OK, body = Ro<DbzTaskVo>))
)]
#[debug_handler]
#[log_call]
pub async fn register(
    headers: HeaderMap,
    Json(mut add_dto): Json<DbzTaskAddDto>,
) -> Result<Json<Ro<DbzTaskVo>>, CtrlError> {
    let current_user_id = get_current_user_id(&headers)?;
    add_dto._current_user_id = current_user_id;
    let ro = DbzTaskSvc::register::<DatabaseTransaction>(add_dto, None).await?;
    Ok(Json(ro))
}

/// # 注销 Debezium 连接器
///
/// 删除 Kafka Connect 连接器、关联的 jobx 任务计划和数据库记录。
#[utoipa::path(
    post,
    path = "/dbz/task/unregister/{id}",
    params(
        ("id" = u64, Path, description = "任务ID")
    ),
    responses((status = OK, body = Ro<DbzTaskVo>))
)]
#[debug_handler]
#[log_call]
pub async fn unregister(
    Path(id): Path<U64>,
    headers: HeaderMap,
) -> Result<Json<Ro<DbzTaskVo>>, CtrlError> {
    get_current_user_id(&headers)?;
    let ro = DbzTaskSvc::unregister::<DatabaseTransaction>(id, None).await?;
    Ok(Json(ro))
}

/// # 查询连接器状态
///
/// 通过 Kafka Connect REST API 查询连接器的实际运行状态。
#[utoipa::path(
    post,
    path = "/dbz/task/check-status/{id}",
    params(
        ("id" = u64, Path, description = "任务ID")
    ),
    responses((status = OK, body = Ro<DbzTaskVo>))
)]
#[debug_handler]
#[log_call]
pub async fn check_status(
    Path(id): Path<U64>,
    headers: HeaderMap,
) -> Result<Json<Ro<DbzTaskVo>>, CtrlError> {
    get_current_user_id(&headers)?;
    let ro = DbzTaskSvc::check_status::<DatabaseTransaction>(id, None).await?;
    Ok(Json(ro))
}

/// # 重启连接器
///
/// 重新向 Kafka Connect 部署连接器。
#[utoipa::path(
    post,
    path = "/dbz/task/restart/{id}",
    params(
        ("id" = u64, Path, description = "任务ID")
    ),
    responses((status = OK, body = Ro<DbzTaskVo>))
)]
#[debug_handler]
#[log_call]
pub async fn restart(
    Path(id): Path<U64>,
    headers: HeaderMap,
) -> Result<Json<Ro<DbzTaskVo>>, CtrlError> {
    get_current_user_id(&headers)?;
    let ro = DbzTaskSvc::restart::<DatabaseTransaction>(id, None).await?;
    Ok(Json(ro))
}