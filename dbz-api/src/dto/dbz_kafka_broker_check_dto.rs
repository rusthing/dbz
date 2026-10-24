//! # Kafka 集群连通性检测 DTO
//!
//! 检测是**主动动作**（会回写 `run_status`），因此用 `POST` 而不是 `GET`；
//! 路径是静态的 `/dbz/kafka-broker/check`，与 CRUD 的 `/dbz/kafka-broker/{id}`
//! 只在静态段上重叠，matchit 里静态段优先。

use robotech::api::U64;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// # 检测请求
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct DbzKafkaBrokerCheckDto {
    /// Kafka 集群ID
    pub id: U64,
}
