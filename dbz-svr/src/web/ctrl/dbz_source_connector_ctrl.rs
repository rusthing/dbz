use crate::svc::{DbzControlledDeleteSvc, DbzNodeType};
use crate::web::ctrl::delete_guard;
use robotech::macros::ctrl;

// 删除接口在这里手写：路径与方法沿用框架注册的 CRUD 删除，但先校验审核口令并记审计。
// 其余 CRUD 接口继续由宏生成。
#[ctrl(skip = [del_by_id, del_by_query_dto])]
struct DbzSourceConnectorCtrl;

/// # 删除上游连接器（需审核口令）
///
/// 删除前校验 `X-Audit-Pswd` 对应的当前用户口令；失败时 HTTP 200 + `result=warn`
/// + 业务码，与 `/dbz/control-plane/node-delete` 一致，且不删除任何一行。
#[utoipa::path(
    delete,
    path = "/dbz/source-connector/{id}",
    params(
        ("id" = U64, Path, description = "记录的唯一标识符")
    ),
    responses((status = OK, body = Ro<DbzSourceConnectorVo>))
)]
#[debug_handler]
#[log_call]
pub async fn del_by_id(
    headers: HeaderMap,
    Path(id): Path<U64>,
) -> Result<Json<Ro<DbzSourceConnectorVo>>, CtrlError> {
    if let Err(failure) = delete_guard::verify(&headers).await {
        return Ok(Json(Ro::warn(failure.msg).code(Some(failure.code))));
    }
    let operator_id = get_current_user_id(&headers)?;
    let node_id = i64::from(id);
    let node_name = DbzControlledDeleteSvc::name_of(DbzNodeType::Source, node_id).await?;
    let ro = DbzSourceConnectorSvc::del_by_id::<DatabaseTransaction>(id, None).await?;
    DbzControlledDeleteSvc::record(DbzNodeType::Source, node_id, &node_name, operator_id).await;
    Ok(Json(ro))
}

/// # 按查询条件批量删除上游连接器（需审核口令）
///
/// 名称在删除前先取出来，删完就查不到了；逐条写审计。
#[utoipa::path(
    delete,
    path = "/dbz/source-connector",
    params(DbzSourceConnectorQueryDto),
    responses((status = OK, description = "已删除"))
)]
#[debug_handler]
#[log_call]
pub async fn del_by_query_dto(
    headers: HeaderMap,
    Query(dto): Query<DbzSourceConnectorQueryDto>,
) -> Result<Json<Ro<()>>, CtrlError> {
    if let Err(failure) = delete_guard::verify(&headers).await {
        return Ok(Json(Ro::warn(failure.msg).code(Some(failure.code))));
    }
    let operator_id = get_current_user_id(&headers)?;
    let matched =
        DbzSourceConnectorSvc::list_by_query_dto::<DatabaseTransaction>(dto.clone(), None).await?;
    let items: Vec<(i64, String)> = matched
        .extra
        .unwrap_or_default()
        .into_iter()
        .map(|one| (i64::from(one.id), one.name))
        .collect();
    let ro = DbzSourceConnectorSvc::del_by_query_dto::<DatabaseTransaction>(dto, None).await?;
    DbzControlledDeleteSvc::record_many(DbzNodeType::Source, &items, operator_id).await;
    Ok(Json(ro))
}
