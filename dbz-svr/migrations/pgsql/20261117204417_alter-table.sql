-- dbz 数据库迁移：alter-table（新增「死信清理墓碑」）
--
-- 背景：清理死信原本只删页面索引，游标在进程内，服务重启后会从 earliest 重扫，
-- 比保留期新的记录会被原样插回来，清理结论被还原。Kafka 侧没有「按条件删单条记录」
-- 的接口（DeleteRecords 只能按分区前缀截断、compaction 按 key 墓碑会误伤同一业务主键的
-- 多条死信），因此在 dbz 侧记墓碑坐标：同步时命中墓碑就跳过。
--
-- 墓碑按 purged_ms 随死信保留期一起过期：早于保留期的记录同步本就会跳过，墓碑不再需要。

/*==============================================================*/
/* Table: dbz_dead_letter_tombstone                             */
/*==============================================================*/
create table dbz_dead_letter_tombstone (
   id                   INT8                 not null,
   connector_type       INT2                 not null default 2,
   connector_id         INT8                 not null default 0,
   connector_name       VARCHAR(255)         not null default '',
   topic                VARCHAR(255)         not null,
   partition            INT4                 not null,
   "offset"             INT8                 not null,
   purged_by            INT8                 null,
   purged_ms            INT8                 not null,
   remark               VARCHAR(500)         null,
   creater_id           INT8                 not null,
   create_ms            INT8                 not null,
   updater_id           INT8                 not null,
   update_ms            INT8                 not null,
   constraint PK_DBZ_DEAD_LETTER_TOMBSTONE primary key (id)
);

comment on table dbz_dead_letter_tombstone is
'死信清理墓碑：按条件清理死信时先记下被删记录的坐标，同步时命中即跳过。Kafka 里的报文不能按条件删，这张表让「已清理」在重启重扫后依然成立；随死信保留期一起过期';

comment on column dbz_dead_letter_tombstone.id is
'主键 ID';

comment on column dbz_dead_letter_tombstone.connector_type is
'连接器类型；Connect 的 DLQ 只适用于 sink，故恒为 2（与死信索引同义）';

comment on column dbz_dead_letter_tombstone.connector_id is
'下游连接器记录 ID（由连接器名反查，查不到记 0）；与 connector_name/topic/partition/offset 组成墓碑唯一键';

comment on column dbz_dead_letter_tombstone.connector_name is
'产生该死信的连接器名（DLQ 上下文头原值）；与 connector_id 一起区分同源 topic 的多条连接器';

comment on column dbz_dead_letter_tombstone.topic is
'原始消息所在的源 topic；墓碑唯一键之一';

comment on column dbz_dead_letter_tombstone.partition is
'原始消息所在分区；墓碑唯一键之一';

comment on column dbz_dead_letter_tombstone."offset" is
'原始消息 offset；墓碑唯一键之一';

comment on column dbz_dead_letter_tombstone.purged_by is
'执行清理的操作人 ID；定时保留清理写入 0';

comment on column dbz_dead_letter_tombstone.purged_ms is
'清理时间戳（毫秒）；墓碑按它随死信保留期一起过期';

comment on column dbz_dead_letter_tombstone.remark is
'清理备注（为什么清掉这批记录）';

comment on column dbz_dead_letter_tombstone.creater_id is
'创建者ID';

comment on column dbz_dead_letter_tombstone.create_ms is
'创建时间戳';

comment on column dbz_dead_letter_tombstone.updater_id is
'更新者ID';

comment on column dbz_dead_letter_tombstone.update_ms is
'更新时间戳';

/*==============================================================*/
/* Index: dbz_dead_letter_tombstone_PK                          */
/*==============================================================*/
create unique index dbz_dead_letter_tombstone_PK on dbz_dead_letter_tombstone (
id
);

/*==============================================================*/
/* Index: ux_dbz_dead_letter_tombstone_key                      */
/*==============================================================*/
create unique index ux_dbz_dead_letter_tombstone_key on dbz_dead_letter_tombstone (
connector_name,
connector_id,
topic,
partition,
"offset"
);

-- 部分 / 表达式 / 性能索引（PowerDesigner 不能自动生成）

CREATE INDEX ix_dbz_dead_letter_tombstone_purged_ms ON dbz_dead_letter_tombstone USING btree (purged_ms);
