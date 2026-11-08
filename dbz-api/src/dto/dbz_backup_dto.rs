//! # Kafka 集群备份 / 恢复 DTO
//!
//! 导出是只读动作，用 `GET` + 查询参数；恢复要写库，用 `POST`。
//!
//! 恢复请求体里的 `file` 用 `serde_json::Value` 接收：文件可能来自旧版本或手工编辑，
//! 交给服务层逐条校验并把原因回给页面，而不是让反序列化直接报一个笼统的 400。

use robotech::api::U64;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

/// # 导出备份的查询参数
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema, IntoParams)]
#[serde(default, rename_all = "camelCase")]
pub struct DbzBackupExportDto {
    /// Kafka 集群 ID
    pub kafka_broker_id: U64,
}

/// # 恢复请求
///
/// `strategy`：`skip` 仅补齐缺失（默认）/ `overwrite` 覆盖已存在记录。
/// `dryRun=true` 只计算计划不写库，供前端预览。
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct DbzBackupRestoreDto {
    /// 备份文件内容（dbz-cluster-backup 结构）
    #[schema(value_type = Object)]
    pub file: serde_json::Value,
    /// 恢复策略：`skip` / `overwrite`
    pub strategy: String,
    /// 只预览、不写库
    pub dry_run: bool,
}
