use robotech::macros::ctrl;

#[ctrl]
struct DbzInstanceCtrl;

/// # 注册 Debezium 实例
///
/// 创建实例记录、创建 jobx 定时检查任务计划，并向 Kafka Connect 注册上游、下游连接器。
///
/// ## 请求体
/// * `DbzInstanceAddDto` - 包含实例信息的结构体（`jobId` 无需传入，由注册流程自动生成）
///
/// ## 请求头
/// * `USER_ID_HEADER_NAME` - 当前用户ID，必需项，类型为u64
///
/// ## 返回值
/// * 成功时返回注册后的实例信息
/// * 失败时返回相应的错误信息
#[utoipa::path(
    post,
    path = "/dbz/instance/register",
    request_body = DbzInstanceAddDto,
    responses((status = OK, body = Ro<DbzInstanceVo>))
)]
#[debug_handler]
#[log_call]
pub async fn register(
    headers: HeaderMap,
    Json(mut add_dto): Json<DbzInstanceAddDto>,
) -> Result<Json<Ro<DbzInstanceVo>>, CtrlError> {
    // 从header中解析当前用户ID，如果没有或解析失败则抛出ValidationError
    add_dto._current_user_id = get_current_user_id(&headers)?;
    // 从header中解析当前时间戳（可选），未提供时由dao层自动填充
    if let Some(ms) = get_current_ms(&headers)? {
        add_dto._current_ms = Some(ms);
    }

    let ro = DbzInstanceSvc::register::<DatabaseTransaction>(add_dto, None).await?;
    Ok(Json(ro))
}

/// # 注销 Debezium 实例
///
/// 删除 Kafka Connect 的上游、下游连接器，删除 jobx 定时检查任务计划，并删除实例记录。
///
/// ## 请求参数
/// * `id` - 待注销实例的唯一标识符，类型为U64
///
/// ## 返回值
/// * 成功时返回被注销的实例信息
/// * 失败时返回相应的错误信息
#[utoipa::path(
    post,
    path = "/dbz/instance/unregister/{id}",
    params(
        ("id" = U64, Path, description = "实例的唯一标识符")
    ),
    responses((status = OK, body = Ro<DbzInstanceVo>))
)]
#[debug_handler]
#[log_call]
pub async fn unregister(
    Path(id): Path<U64>,
    headers: HeaderMap,
) -> Result<Json<Ro<DbzInstanceVo>>, CtrlError> {
    // 从header中解析当前用户ID，如果没有或解析失败则抛出ValidationError
    get_current_user_id(&headers)?;
    let ro = DbzInstanceSvc::unregister::<DatabaseTransaction>(id, None).await?;
    Ok(Json(ro))
}

/// # 查询实例连接器状态
///
/// 通过 Kafka Connect REST API 查询实例上游、下游连接器的实际运行状态并回写；
/// 状态异常时触发告警并尝试自动重启连接器。
///
/// ## 请求参数
/// * `id` - 待检查实例的唯一标识符，类型为U64
///
/// ## 返回值
/// * 成功时返回更新后的实例信息
/// * 失败时返回相应的错误信息
#[utoipa::path(
    post,
    path = "/dbz/instance/check-status/{id}",
    params(
        ("id" = U64, Path, description = "实例的唯一标识符")
    ),
    responses((status = OK, body = Ro<DbzInstanceVo>))
)]
#[debug_handler]
#[log_call]
pub async fn check_status(
    Path(id): Path<U64>,
    headers: HeaderMap,
) -> Result<Json<Ro<DbzInstanceVo>>, CtrlError> {
    // 从header中解析当前用户ID，如果没有或解析失败则抛出ValidationError
    get_current_user_id(&headers)?;
    let ro = DbzInstanceSvc::check_status::<DatabaseTransaction>(id, None).await?;
    Ok(Json(ro))
}

/// # 重启实例连接器
///
/// 重新向 Kafka Connect 部署实例的上游、下游连接器并回写运行状态。
///
/// ## 请求参数
/// * `id` - 待重启实例的唯一标识符，类型为U64
///
/// ## 返回值
/// * 成功时返回更新后的实例信息
/// * 失败时返回相应的错误信息
#[utoipa::path(
    post,
    path = "/dbz/instance/restart/{id}",
    params(
        ("id" = U64, Path, description = "实例的唯一标识符")
    ),
    responses((status = OK, body = Ro<DbzInstanceVo>))
)]
#[debug_handler]
#[log_call]
pub async fn restart(
    Path(id): Path<U64>,
    headers: HeaderMap,
) -> Result<Json<Ro<DbzInstanceVo>>, CtrlError> {
    // 从header中解析当前用户ID，如果没有或解析失败则抛出ValidationError
    get_current_user_id(&headers)?;
    let ro = DbzInstanceSvc::restart::<DatabaseTransaction>(id, None).await?;
    Ok(Json(ro))
}
