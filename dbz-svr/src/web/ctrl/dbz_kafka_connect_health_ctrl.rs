//! # Kafka Connect 健康检查控制器
//!
//! 一个手写接口：`GET /dbz/kafka-connect/health`，探测目标 Connect 是否可达，
//! 并带回版本、worker 数、连接器数等信息供前端展示。
//!
//! 与 [`dbz_kafka_connect_ctrl`](crate::web::ctrl::dbz_kafka_connect_ctrl)（CRUD）分开，
//! 是因为 `#[router(crud)]` 与 `#[router(routes ...)]` 各自生成一棵路由树、各自按
//! **结构体名**推导 ctrl 模块，一个 ctrl 模块只能被一棵树使用。

use crate::dto::DbzKafkaConnectHealthDto;
use crate::svc::DbzKafkaConnectSvc;
use crate::vo::DbzKafkaConnectHealthVo;
use axum::debug_handler;
use axum::extract::Query;
use axum::response::Json;
use robotech::api::Ro;
use robotech::macros::log_call;
use robotech::web::CtrlError;

/// # 探测 Kafka Connect 自身健康状态
///
/// 可达性以 `GET /connectors` 为准，版本等信息来自 Connect 根路径。只读接口，
/// 不回写库内状态；连不上属于业务结论，返回 200 + `reachable=false` 与原因。
///
/// ## 查询参数
/// * `kafkaConnectId` - Kafka Connect ID
///
/// ## 返回值
/// * `Ro<DbzKafkaConnectHealthVo>` - 可达性、版本、worker 数、连接器数、耗时、原因
#[utoipa::path(
    get,
    path = "/dbz/kafka-connect/health",
    params(DbzKafkaConnectHealthDto),
    responses((status = OK, body = Ro<DbzKafkaConnectHealthVo>))
)]
#[debug_handler]
#[log_call]
pub async fn health(
    Query(dto): Query<DbzKafkaConnectHealthDto>,
) -> Result<Json<Ro<DbzKafkaConnectHealthVo>>, CtrlError> {
    let ro = DbzKafkaConnectSvc::health(dto).await?;
    Ok(Json(ro))
}
