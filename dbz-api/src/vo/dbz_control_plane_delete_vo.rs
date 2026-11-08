//! # 控制面节点删除结果 VO

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// # 本次删除确认实际校验的口令类型
///
/// 审核口令为空时由 adm-rs 回退校验登录口令；是否回退只看 `adm_user.audit_pswd`，
/// dbz 只透传结果，不参与判定。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum DbzDeletePswdKind {
    /// 已设置审核口令：校验的是审核口令
    Audit,
    /// 未设置审核口令：回退校验登录口令
    Login,
}

/// # 控制面节点删除结果
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzControlPlaneNodeDeleteVo {
    /// 本次实际校验的口令类型，仅用于前端提示文案
    pub pswd_kind: DbzDeletePswdKind,
}
