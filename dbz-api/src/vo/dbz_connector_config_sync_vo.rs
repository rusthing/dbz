//! # 连接器配置同步结果

use crate::vo::DbzDeletePswdKind;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DbzConnectorConfigSyncVo {
    /// 本次实际校验的口令类型；`toLocal` 不校验口令时为空
    pub pswd_kind: Option<DbzDeletePswdKind>,
    /// 同步后重新计算的检测标记
    pub inconsistent: bool,
    /// 同步后重新计算的对比快照（脱敏 JSON 文本）；一致时为空
    pub inconsistent_detail: Option<String>,
}
