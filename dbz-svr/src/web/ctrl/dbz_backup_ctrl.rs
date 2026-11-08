//! # Kafka 集群备份 / 恢复控制器
//!
//! 两个手写接口：
//!
//! * `GET /dbz/backup/export` - 导出整个 Kafka 集群（含 Connect 与连接器）为 JSON；
//! * `POST /dbz/backup/restore` - 从备份 JSON 恢复库内纳管记录，`dryRun` 时只预览。
//!
//! 独立成一组路由，不与 `dbz_kafka_broker_ctrl` 的 CRUD 混用：`#[router(crud)]` 与
//! `#[router(routes ...)]` 各自生成一棵路由树，一个 ctrl 模块只能被一棵树使用。

use crate::dto::{DbzBackupExportDto, DbzBackupRestoreDto};
use crate::svc::DbzBackupSvc;
use crate::vo::{DbzBackupFileVo, DbzBackupRestoreVo};
use axum::debug_handler;
use axum::extract::Query;
use axum::response::Json;
use robotech::api::Ro;
use robotech::web::CtrlError;

/// # 导出 Kafka 集群备份
///
/// 导出范围是整个集群：broker 配置（**含 `sasl_password`**）、其下全部 Connect、
/// 以及全部上下游连接器。只生成 JSON 返回给前端下载，服务端不留档。
///
/// ## 查询参数
/// * `kafkaBrokerId` - Kafka 集群 ID
///
/// ## 返回值
/// * `Ro<DbzBackupFileVo>` - 可直接下载的备份文件内容
#[utoipa::path(
    get,
    path = "/dbz/backup/export",
    params(DbzBackupExportDto),
    responses((status = OK, body = Ro<DbzBackupFileVo>))
)]
#[debug_handler]
pub async fn export(
    Query(dto): Query<DbzBackupExportDto>,
) -> Result<Json<Ro<DbzBackupFileVo>>, CtrlError> {
    let ro = DbzBackupSvc::export(dto).await?;
    Ok(Json(ro))
}

/// # 恢复 Kafka 集群备份
///
/// 只写库内纳管记录，**不**在远端注册或删除连接器。`strategy` 为 `skip`（默认，
/// 仅补齐缺失）或 `overwrite`（覆盖已存在）；`dryRun=true` 只计算计划，供页面预览。
///
/// ## 请求体
/// * `DbzBackupRestoreDto` - 备份文件、策略与是否预览
///
/// ## 返回值
/// * `Ro<DbzBackupRestoreVo>` - 校验结论、明细与计数
#[utoipa::path(
    post,
    path = "/dbz/backup/restore",
    request_body = DbzBackupRestoreDto,
    responses((status = OK, body = Ro<DbzBackupRestoreVo>))
)]
#[debug_handler]
pub async fn restore(
    Json(dto): Json<DbzBackupRestoreDto>,
) -> Result<Json<Ro<DbzBackupRestoreVo>>, CtrlError> {
    let ro = DbzBackupSvc::restore(dto).await?;
    Ok(Json(ro))
}
