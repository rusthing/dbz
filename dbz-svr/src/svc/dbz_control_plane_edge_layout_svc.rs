//! # 控制面连线折点服务
//!
//! 连线由归属关系自动生成，用户可上下拖动中间折点（改 `mid_y`）、左右拖动两端
//! 连接点（改 `start_dx` / `end_dx`），上下游归属与连线本身都不可改。按 `edge_id`
//! 单独存一行，拖完即落库，刷新后恢复。

use crate::dao::DbzControlPlaneEdgeLayoutDao;
use crate::dto::{DbzControlPlaneEdgeBendDeleteDto, DbzControlPlaneEdgeBendUpsertDto};
use crate::mo::dbz_control_plane_edge_layout::{
    ActiveModel, Column as EdgeColumn, Entity as EdgeEntity,
};
use crate::vo::DbzControlPlaneEdgeBendVo;
use anyhow::anyhow;
use robotech::api::{Ro, U64};
use robotech::db::get_db_conn;
use robotech::svc::SvcError;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, Set};
use wheel_rs::time_utils::now_ms;

/// # 控制面连线折点服务
pub struct DbzControlPlaneEdgeLayoutSvc;

impl DbzControlPlaneEdgeLayoutSvc {
    /// # 读取全部连线折点
    pub async fn list() -> Result<Ro<Vec<DbzControlPlaneEdgeBendVo>>, SvcError> {
        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;
        let models = EdgeEntity::find()
            .all(db.as_ref())
            .await
            .map_err(|e| anyhow!("读取连线折点失败: {e}"))?;
        let list = models
            .into_iter()
            .map(|model| DbzControlPlaneEdgeBendVo {
                edge_id: model.edge_id,
                mid_y: model.mid_y,
                start_dx: model.start_dx,
                end_dx: model.end_dx,
                update_ms: model.update_ms,
            })
            .collect();
        Ok(Ro::success("查询连线折点成功".to_string()).extra(Some(list)))
    }

    /// # 保存一条连线的折点
    pub async fn upsert(
        dto: DbzControlPlaneEdgeBendUpsertDto,
        operator_id: U64,
    ) -> Result<Ro<()>, SvcError> {
        let edge_id = dto.edge_id.trim().to_string();
        if edge_id.is_empty() || edge_id.len() > 255 {
            return Err(anyhow!("edgeId 不能为空且长度不超过 255").into());
        }
        if !dto.mid_y.is_finite() {
            return Err(anyhow!("折点坐标不合法").into());
        }
        if dto.start_dx.is_some_and(|value| !value.is_finite())
            || dto.end_dx.is_some_and(|value| !value.is_finite())
        {
            return Err(anyhow!("连接点偏移不合法").into());
        }

        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;
        let now = now_ms() as i64;
        let operator = i64::from(operator_id);

        if let Some(model) = EdgeEntity::find()
            .filter(EdgeColumn::EdgeId.eq(edge_id.as_str()))
            .one(db.as_ref())
            .await
            .map_err(|e| anyhow!("查询连线折点失败: {e}"))?
        {
            let mut active: ActiveModel = model.into();
            active.mid_y = Set(dto.mid_y);
            // 只拖折点时不带连接点字段，保留库里已有偏移，避免被重置为 0
            if let Some(value) = dto.start_dx {
                active.start_dx = Set(value);
            }
            if let Some(value) = dto.end_dx {
                active.end_dx = Set(value);
            }
            active.updater_id = Set(operator);
            active.update_ms = Set(now);
            DbzControlPlaneEdgeLayoutDao::update(active, db.as_ref())
                .await
                .map_err(|e| anyhow!("保存连线折点失败: {e}"))?;
            return Ok(Ro::success("已保存连线折点".to_string()));
        }

        let active = ActiveModel {
            edge_id: Set(edge_id),
            mid_y: Set(dto.mid_y),
            start_dx: Set(dto.start_dx.unwrap_or(0.0)),
            end_dx: Set(dto.end_dx.unwrap_or(0.0)),
            creater_id: Set(operator),
            create_ms: Set(now),
            updater_id: Set(operator),
            update_ms: Set(now),
            ..Default::default()
        };
        DbzControlPlaneEdgeLayoutDao::insert(active, db.as_ref())
            .await
            .map_err(|e| anyhow!("保存连线折点失败: {e}"))?;
        Ok(Ro::success("已保存连线折点".to_string()))
    }

    /// # 删除一条连线的折点（恢复默认居中走线）
    pub async fn delete(dto: DbzControlPlaneEdgeBendDeleteDto) -> Result<Ro<()>, SvcError> {
        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;
        EdgeEntity::delete_many()
            .filter(EdgeColumn::EdgeId.eq(dto.edge_id.trim()))
            .exec(db.as_ref())
            .await
            .map_err(|e| anyhow!("删除连线折点失败: {e}"))?;
        Ok(Ro::success("已恢复默认走线".to_string()))
    }
}
