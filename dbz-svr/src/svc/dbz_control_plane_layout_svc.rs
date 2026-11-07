//! # 控制面画布布局服务
//!
//! 画布位置按「节点类型 + 节点 ID」逐条落库。读取时一次返回全部位置，前端把
//! 找不到位置的新节点按默认网格摆放；保存是单节点 upsert，拖动一个节点只写一行。

use crate::dao::DbzControlPlaneLayoutDao;
use crate::dto::{DbzControlPlaneLayoutDeleteDto, DbzControlPlaneLayoutUpsertDto};
use crate::mo::dbz_control_plane_layout::{
    ActiveModel, Column as LayoutColumn, Entity as LayoutEntity,
};
use crate::vo::DbzControlPlaneLayoutVo;
use anyhow::anyhow;
use robotech::api::{Ro, U64};
use robotech::db::get_db_conn;
use robotech::svc::SvcError;
use sea_orm::{ColumnTrait, Condition, EntityTrait, QueryFilter, QueryOrder, Set};
use wheel_rs::time_utils::now_ms;

/// # 控制面布局服务
///
/// 只有列表、保存、删除三个动作，不生成通用 CRUD 路由。
pub struct DbzControlPlaneLayoutSvc;

impl DbzControlPlaneLayoutSvc {
    /// # 读取全部节点位置
    pub async fn list() -> Result<Ro<Vec<DbzControlPlaneLayoutVo>>, SvcError> {
        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;
        let models = LayoutEntity::find()
            .order_by_asc(LayoutColumn::NodeType)
            .order_by_asc(LayoutColumn::NodeId)
            .all(db.as_ref())
            .await
            .map_err(|e| anyhow!("读取控制面布局失败: {e}"))?;

        let list = models
            .into_iter()
            .map(|model| DbzControlPlaneLayoutVo {
                node_type: model.node_type.into(),
                node_id: U64::from(model.node_id),
                x: model.x,
                y: model.y,
                update_ms: model.update_ms,
            })
            .collect();
        Ok(Ro::success("查询控制面布局成功".to_string()).extra(Some(list)))
    }

    /// # 保存一个节点的位置
    ///
    /// 同一节点重复保存时只更新坐标与更新人/时间，不新增行。
    pub async fn upsert(
        dto: DbzControlPlaneLayoutUpsertDto,
        operator_id: U64,
    ) -> Result<Ro<()>, SvcError> {
        let node_id = i64::from(dto.node_id);
        if node_id <= 0 {
            return Err(anyhow!("nodeId 必须为正数").into());
        }
        if !dto.x.is_finite() || !dto.y.is_finite() {
            return Err(anyhow!("节点坐标不合法").into());
        }

        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;
        let now = now_ms() as i64;
        let operator = i64::from(operator_id);
        let node_type = i16::from(dto.node_type);

        let condition = Condition::all()
            .add(LayoutColumn::NodeType.eq(node_type))
            .add(LayoutColumn::NodeId.eq(node_id));

        if let Some(model) = LayoutEntity::find()
            .filter(condition)
            .one(db.as_ref())
            .await
            .map_err(|e| anyhow!("查询控制面布局失败: {e}"))?
        {
            let mut active: ActiveModel = model.into();
            active.x = Set(dto.x);
            active.y = Set(dto.y);
            active.updater_id = Set(operator);
            active.update_ms = Set(now);
            DbzControlPlaneLayoutDao::update(active, db.as_ref())
                .await
                .map_err(|e| anyhow!("保存控制面布局失败: {e}"))?;
            return Ok(Ro::success("已保存节点位置".to_string()));
        }

        let active = ActiveModel {
            node_type: Set(node_type),
            node_id: Set(node_id),
            x: Set(dto.x),
            y: Set(dto.y),
            creater_id: Set(operator),
            create_ms: Set(now),
            updater_id: Set(operator),
            update_ms: Set(now),
            ..Default::default()
        };
        DbzControlPlaneLayoutDao::insert(active, db.as_ref())
            .await
            .map_err(|e| anyhow!("保存控制面布局失败: {e}"))?;
        Ok(Ro::success("已保存节点位置".to_string()))
    }

    /// # 删除一个节点的位置
    ///
    /// 节点记录被删除后清掉对应位置，避免留下对不上业务记录的布局行。
    pub async fn delete(dto: DbzControlPlaneLayoutDeleteDto) -> Result<Ro<()>, SvcError> {
        let node_id = i64::from(dto.node_id);
        if node_id <= 0 {
            return Err(anyhow!("nodeId 必须为正数").into());
        }
        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;
        LayoutEntity::delete_many()
            .filter(
                Condition::all()
                    .add(LayoutColumn::NodeType.eq(i16::from(dto.node_type)))
                    .add(LayoutColumn::NodeId.eq(node_id)),
            )
            .exec(db.as_ref())
            .await
            .map_err(|e| anyhow!("删除控制面布局失败: {e}"))?;
        Ok(Ro::success("已删除节点位置".to_string()))
    }
}
