//! # Kafka Connect 检测控制器
//!
//! 一个手写接口：`POST /dbz/kafka-connect/detect`，以 Connect 上的实际连接器为准
//! 同步库内纳管记录，并回写 `inconsistent` 检测标记。
//!
//! 与 [`dbz_kafka_connect_ctrl`](crate::web::ctrl::dbz_kafka_connect_ctrl)（CRUD）分开，
//! 是因为 `#[router(crud)]` 与 `#[router(routes ...)]` 各自生成一棵路由树、各自按
//! **结构体名**推导 ctrl 模块，一个 ctrl 模块只能被一棵树使用。

use crate::dto::DbzKafkaConnectDetectDto;
use crate::svc::DbzKafkaConnectSvc;
use crate::vo::DbzKafkaConnectDetectVo;
use axum::debug_handler;
use axum::response::Json;
use robotech::api::Ro;
use robotech::macros::log_call;
use robotech::web::CtrlError;

/// # 检测并同步 Kafka Connect 连接器
///
/// 只比对 `connectorConfig` 与远端存在性，**不看运行状态**，避免状态抖动误报：
/// 远端有、库内没有 → 补录；配置不一致 → 标记 `inconsistent`；远端已不存在 →
/// 置为禁用并标记。不删除库内记录，也不在远端注册或删除连接器。
///
/// ## 请求体
/// * `DbzKafkaConnectDetectDto` - Kafka Connect ID
///
/// ## 返回值
/// * `Ro<DbzKafkaConnectDetectVo>` - 补录/标记/禁用/恢复/跳过数量与明细
#[utoipa::path(
    post,
    path = "/dbz/kafka-connect/detect",
    request_body = DbzKafkaConnectDetectDto,
    responses((status = OK, body = Ro<DbzKafkaConnectDetectVo>))
)]
#[debug_handler]
#[log_call]
pub async fn detect(
    Json(dto): Json<DbzKafkaConnectDetectDto>,
) -> Result<Json<Ro<DbzKafkaConnectDetectVo>>, CtrlError> {
    let ro = DbzKafkaConnectSvc::detect(dto).await?;
    Ok(Json(ro))
}
