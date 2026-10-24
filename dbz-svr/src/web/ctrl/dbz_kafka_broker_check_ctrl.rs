//! # Kafka 集群检测控制器
//!
//! 一个手写接口：`POST /dbz/kafka-broker/check`，连一次 broker 拉 topic 列表，顺便把
//! 状态写回记录。
//!
//! 与 [`dbz_kafka_broker_ctrl`](crate::web::ctrl::dbz_kafka_broker_ctrl)（CRUD）分开，
//! 是因为 `#[router(crud)]` 与 `#[router(routes ...)]` 各自生成一棵路由树、各自按
//! **结构体名**推导 ctrl 模块，一个 ctrl 模块只能被一棵树使用。

use crate::dto::DbzKafkaBrokerCheckDto;
use crate::svc::DbzKafkaBrokerSvc;
use crate::vo::DbzKafkaBrokerCheckVo;
use axum::debug_handler;
use axum::response::Json;
use robotech::api::Ro;
use robotech::macros::log_call;
use robotech::web::CtrlError;

/// # 检测 Kafka 集群连通性
///
/// 真的发一次 metadata 请求（不只是探端口）：能拉到 topic 列表才算通。结果里带
/// topic 数量与耗时，方便判断「确实连上了」而不只是「没报错」。
///
/// 检测会回写记录的 `run_status`；连不上属于业务结论，因此返回 200 + `reachable=false`
/// 与失败原因，而不是报接口错误。
///
/// ## 请求体
/// * `DbzKafkaBrokerCheckDto` - Kafka 集群ID
///
/// ## 返回值
/// * `Ro<DbzKafkaBrokerCheckVo>` - 是否连通、topic 数量、耗时、失败原因
#[utoipa::path(
    post,
    path = "/dbz/kafka-broker/check",
    request_body = DbzKafkaBrokerCheckDto,
    responses((status = OK, body = Ro<DbzKafkaBrokerCheckVo>))
)]
#[debug_handler]
#[log_call]
pub async fn check_broker(
    Json(dto): Json<DbzKafkaBrokerCheckDto>,
) -> Result<Json<Ro<DbzKafkaBrokerCheckVo>>, CtrlError> {
    let ro = DbzKafkaBrokerSvc::check(dto).await?;
    Ok(Json(ro))
}
