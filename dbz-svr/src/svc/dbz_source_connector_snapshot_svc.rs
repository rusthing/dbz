//! # 上游连接器「再同步」服务
//!
//! 把「再全量同步一次」包成两步：预检（只读，列出会快照哪些表）与触发（往源库
//! 信号表写一条 `execute-snapshot`）。为什么要直连源库、为什么不用 Kafka Connect
//! 的接口，见 [`DbzSignalUtils`](crate::utils::DbzSignalUtils) 的模块文档。
//!
//! 与实体服务不同，本服务**不写本库**：读一条 `dbz_source_connector` 记录取配置，
//! 改的是源库里的信号表。因此不走 `#[svc]` 宏，手写 `impl` 块。

use crate::dto::{DbzSourceConnectorSnapshotDto, DbzSourceConnectorSnapshotPreviewDto};
use crate::svc::DbzSourceConnectorSvc;
use crate::utils::{DbzSignalUtils, SourceSignalConfig};
use crate::vo::{DbzSourceConnectorSnapshotVo, DbzSourceConnectorVo};
use anyhow::{anyhow, bail};
use robotech::api::{Ro, U64};
use robotech::macros::log_call;
use sea_orm::DatabaseConnection;
use tracing::info;

/// # 上游连接器再同步服务
pub struct DbzSourceConnectorSnapshotSvc;

impl DbzSourceConnectorSnapshotSvc {
    /// # 预检：这次会快照哪些表
    ///
    /// 只解析配置 + 读一次源库的 `information_schema`，不写任何东西。前端拿它填
    /// 「选择表」的下拉，也用它把人挡在「连接器配了信号表吗」「dbz 连得上源库吗」
    /// 这些问题之前 —— 真发信号时才发现连不上，体验太差。
    ///
    /// ## 参数
    /// * `dto` - 上游连接器ID
    ///
    /// ## 返回值
    /// * `Ok(Ro<DbzSourceConnectorSnapshotVo>)` - 方言、库、信号表、表清单与提醒
    /// * `Err(anyhow::Error)` - 记录不存在/已禁用、配置不支持、连不上源库
    #[log_call]
    pub async fn preview(
        dto: DbzSourceConnectorSnapshotPreviewDto,
    ) -> Result<Ro<DbzSourceConnectorSnapshotVo>, anyhow::Error> {
        let (source, config) = Self::load(dto.id).await?;
        let captured = DbzSignalUtils::captured_tables(&config).await?;
        if captured.tables.is_empty() {
            bail!(
                "连接器<{}>的捕获范围里没有可快照的表：检查 database.include.list / \
                 table.include.list 是否写得太窄",
                source.name
            );
        }

        let vo = DbzSourceConnectorSnapshotVo {
            dialect: config.dialect.value().to_string(),
            database: config.database.clone(),
            signal_collection: config.signal_collection.clone(),
            data_collections: captured.tables,
            warnings: captured.warnings,
        };
        Ok(
            Ro::success(format!("解析到 {} 张可快照的表", vo.data_collections.len()))
                .extra(Some(vo)),
        )
    }

    /// # 触发快照（写信号）
    ///
    /// 把表清单拼成 `execute-snapshot` 信号写进源库的信号表。调用方没指定表时用
    /// 捕获范围内的全部表；指定了就逐个核对是否在捕获范围内 —— Debezium 对不在
    /// 捕获范围的表只会打一行日志然后跳过，不如这里直接拒绝。
    ///
    /// 返回成功**只代表信号已写入**：连接器要等自己读到这条变更才动手，中间隔着一段
    /// 位点推进，所以这里不说「快照已开始」。
    ///
    /// ## 参数
    /// * `dto` - 上游连接器ID + 表清单（可空）+ 快照类型
    ///
    /// ## 返回值
    /// * `Ok(Ro<()>)` - 信号已写入，`msg` 里带信号 id 与表数量
    /// * `Err(anyhow::Error)` - 表不在捕获范围、连不上源库、写信号失败
    #[log_call]
    pub async fn snapshot(dto: DbzSourceConnectorSnapshotDto) -> Result<Ro<()>, anyhow::Error> {
        let (source, config) = Self::load(dto.id).await?;
        let captured = DbzSignalUtils::captured_tables(&config).await?;

        let tables = if dto.data_collections.is_empty() {
            captured.tables
        } else {
            let requested =
                DbzSignalUtils::normalize_data_collections(&config, &dto.data_collections)?;
            let unknown: Vec<&str> = requested
                .iter()
                .filter(|table| !captured.tables.contains(table))
                .map(String::as_str)
                .collect();
            if !unknown.is_empty() {
                bail!(
                    "以下表不在连接器<{}>的捕获范围内：{}。可先用预检接口看看捕获范围",
                    source.name,
                    unknown.join("、")
                );
            }
            requested
        };
        if tables.is_empty() {
            bail!(
                "连接器<{}>的捕获范围里没有可快照的表：检查 database.include.list / \
                 table.include.list 是否写得太窄",
                source.name
            );
        }

        let snapshot_type = dto.snapshot_type;
        let signal_id =
            DbzSignalUtils::send_snapshot_signal(&config, &tables, snapshot_type).await?;

        info!(
            "上游连接器<{}>(id={}): 已写入 {} 快照信号<{}>，共 {} 张表",
            source.name,
            source.id.value(),
            snapshot_type.value(),
            signal_id,
            tables.len()
        );
        Ok(Ro::success(format!(
            "已写入 {} 快照信号<{}>（{} 张表，信号表 {}）：Debezium 读到后开始快照，\
             进度可在「远端连接器」里看任务状态；快照数据以 op=r 追加到原 topic，\
             下游需按主键幂等处理",
            snapshot_type.value(),
            signal_id,
            tables.len(),
            config.signal_collection
        )))
    }

    /// # 读记录并解析连接参数
    ///
    /// 两个接口的共同前置：记录得在、连接器得是启用状态、配置得能解析出信号表。
    /// 禁用时直接拒绝 —— 信号会留在源库表里，等连接器启用后才被读走，容易让人误以为
    /// 「点了没反应」。
    ///
    /// ## 参数
    /// * `id` - 上游连接器ID
    ///
    /// ## 返回值
    /// * `Ok((DbzSourceConnectorVo, SourceSignalConfig))` - 记录 + 源库信号配置
    /// * `Err(anyhow::Error)` - 不存在、已禁用或配置不支持
    async fn load(id: U64) -> Result<(DbzSourceConnectorVo, SourceSignalConfig), anyhow::Error> {
        let source = DbzSourceConnectorSvc::get_by_id::<DatabaseConnection>(id, None)
            .await
            .map_err(|e| anyhow!("查询上游连接器<{}>失败: {e}", id.value()))?
            .extra
            .ok_or_else(|| anyhow!("上游连接器<{}>不存在", id.value()))?;

        if !source.enabled {
            bail!(
                "上游连接器<{}>已禁用：信号会积压在源库的信号表里，等连接器启用后才被读取，\
                 请先启用再触发",
                source.name
            );
        }

        let config = DbzSignalUtils::parse_connector_config(&source.connector_config)?;
        Ok((source, config))
    }
}
