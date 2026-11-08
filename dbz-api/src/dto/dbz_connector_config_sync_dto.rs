//! # 连接器配置同步 DTO
//!
//! 「不一致」对比弹窗里的双向同步：
//!
//! - `toRemote`：以库内配置为准，把（编辑后的）配置 `PUT` 到远端 Connect；
//! - `toLocal`：以远端实际配置为准，把（编辑后的）配置写回库内纳管记录。
//!
//! 只提交用户实际改过的字段（`patch`）：敏感字段在前端是脱敏的，留空表示保持
//! 原值，后端从当前配置里补回，不会用占位符覆盖真实凭据。

use robotech::api::U64;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use utoipa::ToSchema;

/// # 同步一个连接器的配置
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzConnectorConfigSyncDto {
    /// 节点类型：source / sink
    /// @len 1..32
    pub node_type: String,
    /// 节点记录 ID
    pub node_id: U64,
    /// 同步方向：toRemote（库内为准下发远端）/ toLocal（远端为准写回库内）
    /// @len 1..32
    pub direction: String,
    /// 用户改过的字段，只包含改动项；不传表示原样同步
    #[serde(default)]
    pub patch: Option<Map<String, Value>>,
    /// 当前登录用户审核口令的 SM3 摘要（64 位小写十六进制）
    ///
    /// 仅 `toRemote` 需要：下发会更新远端连接器并触发重载，风险与删除相当。
    /// `toLocal` 只写库内记录，不要求口令。
    #[serde(default)]
    pub audit_pswd: Option<String>,
}
