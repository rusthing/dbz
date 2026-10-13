/*==============================================================*/
/* DBMS name:      PostgreSQL 9.x                               */
/* Created on:     2026/10/13 15:11:34                          */
/*==============================================================*/


/*==============================================================*/
/* Table: dbz_instance                                          */
/*==============================================================*/
create table dbz_instance (
   id                   INT8                 not null,
   name                 VARCHAR(50)          not null,
   "Kafka Connect 地址"   VARCHAR(100)         not null,
   source_connector_name VARCHAR(50)          not null,
   source_connector_config TEXT                 not null,
   sink_connector_name  VARCHAR(50)          not null,
   sink_connector_config TEXT                 not null,
   run_status           INT2                 null,
   remark               VARCHAR(50)          null,
   enabled              BOOL                 not null default true,
   creater_id           INT8                 not null,
   create_ms            INT8                 not null,
   updater_id           INT8                 not null,
   update_ms            INT8                 not null,
   constraint PK_DBZ_INSTANCE primary key (id),
   constraint AK_NAME_DBZ_INSTANCE unique (name),
   constraint AK_SOURCE_CONNECTOR_NAME_DBZ_INSTANCE unique (source_connector_name),
   constraint AK_SINK_CONNECTOR_NAME_DBZ_INSTANCE unique (sink_connector_name)
);

comment on table dbz_instance is
'实例';

comment on column dbz_instance.id is
'ID';

comment on column dbz_instance.name is
'名称';

comment on column dbz_instance."Kafka Connect 地址" is
'Kafka Connect 地址';

comment on column dbz_instance.source_connector_name is
'上游连接器名称';

comment on column dbz_instance.source_connector_config is
'上游连接器配置(JSON)';

comment on column dbz_instance.sink_connector_name is
'下游连接器名称';

comment on column dbz_instance.sink_connector_config is
'下游连接器配置(JSON)';

comment on column dbz_instance.run_status is
'运行状态
0-运行中
1-已禁用
2-连接KafkaConnect失败
3-注册下游连接器失败
4-检测下游连接器失败
5-检测下游连接器异常
6-检测下游连接器状态失败
7-检测下游连接器状态异常
8-注册上游连接器失败
9-检测上游连接器失败
10-检测上游连接器异常
11-检测上游连接器状态失败
12-检测上游连接器状态异常';

comment on column dbz_instance.remark is
'备注';

comment on column dbz_instance.enabled is
'启用';

comment on column dbz_instance.creater_id is
'创建者ID';

comment on column dbz_instance.create_ms is
'创建时间戳';

comment on column dbz_instance.updater_id is
'更新者ID';

comment on column dbz_instance.update_ms is
'更新时间戳';

/*==============================================================*/
/* Index: dbz_instance_PK                                       */
/*==============================================================*/
create unique index dbz_instance_PK on dbz_instance (
id
);

