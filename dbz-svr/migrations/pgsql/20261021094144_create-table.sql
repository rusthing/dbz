/*==============================================================*/
/* DBMS name:      PostgreSQL 9.x                               */
/* Created on:     2026/10/21 9:41:44                           */
/*==============================================================*/


/*==============================================================*/
/* Table: dbz_kafka_connect                                     */
/*==============================================================*/
create table dbz_kafka_connect (
                                   id                   INT8                 not null,
                                   name                 VARCHAR(50)          not null,
                                   url                  VARCHAR(100)         not null,
                                   run_status           INT2                 not null,
                                   remark               VARCHAR(50)          null,
                                   enabled              BOOL                 not null default true,
                                   creater_id           INT8                 not null,
                                   create_ms            INT8                 not null,
                                   updater_id           INT8                 not null,
                                   update_ms            INT8                 not null,
                                   constraint PK_DBZ_KAFKA_CONNECT primary key (id),
                                   constraint AK_NAME_DBZ_KAFKA_CONNECT unique (name),
                                   constraint AK_URL_DBZ_KAFKA_CONNECT unique (url)
);

comment on table dbz_kafka_connect is
'Kafka Connect';

comment on column dbz_kafka_connect.id is
'ID';

comment on column dbz_kafka_connect.name is
'名称';

comment on column dbz_kafka_connect.url is
'地址';

comment on column dbz_kafka_connect.run_status is
'状态
0-无状态(刚添加记录)
1-正常
2-已禁用
3-连接失败';

comment on column dbz_kafka_connect.remark is
'备注';

comment on column dbz_kafka_connect.enabled is
'启用';

comment on column dbz_kafka_connect.creater_id is
'创建者ID';

comment on column dbz_kafka_connect.create_ms is
'创建时间戳';

comment on column dbz_kafka_connect.updater_id is
'更新者ID';

comment on column dbz_kafka_connect.update_ms is
'更新时间戳';

/*==============================================================*/
/* Index: dbz_kafka_connect_PK                                  */
/*==============================================================*/
create unique index dbz_kafka_connect_PK on dbz_kafka_connect (
                                                               id
    );

/*==============================================================*/
/* Table: dbz_sink_connector                                    */
/*==============================================================*/
create table dbz_sink_connector (
                                    id                   INT8                 not null,
                                    kafka_connect_id     INT8                 not null,
                                    source_connector_id  INT8                 not null,
                                    name                 VARCHAR(50)          not null,
                                    connector_name       VARCHAR(50)          not null,
                                    connector_config     TEXT                 not null,
                                    run_status           INT2                 not null,
                                    remark               VARCHAR(50)          null,
                                    enabled              BOOL                 not null default true,
                                    creater_id           INT8                 not null,
                                    create_ms            INT8                 not null,
                                    updater_id           INT8                 not null,
                                    update_ms            INT8                 not null,
                                    constraint PK_DBZ_SINK_CONNECTOR primary key (id),
                                    constraint AK_NAME_DBZ_SINK_CONNECTOR unique (name),
                                    constraint AK_CONNECTOR_NAME_DBZ_SINK_CONNECTOR unique (connector_name)
);

comment on table dbz_sink_connector is
'下游连接器';

comment on column dbz_sink_connector.id is
'ID';

comment on column dbz_sink_connector.kafka_connect_id is
'Kafka Connect ID';

comment on column dbz_sink_connector.source_connector_id is
'上游连接器ID';

comment on column dbz_sink_connector.name is
'名称';

comment on column dbz_sink_connector.connector_name is
'连接器名称';

comment on column dbz_sink_connector.connector_config is
'连接器配置(JSON)';

comment on column dbz_sink_connector.run_status is
'运行状态
0-无状态(刚添加记录)
1-运行中
2-已禁用
3-连接KafkaConnect失败
4-注册连接器失败
5-检测连接器失败
6-检测连接器异常
7-检测连接器状态失败
8-检测连接器状态异常';

comment on column dbz_sink_connector.remark is
'备注';

comment on column dbz_sink_connector.enabled is
'启用';

comment on column dbz_sink_connector.creater_id is
'创建者ID';

comment on column dbz_sink_connector.create_ms is
'创建时间戳';

comment on column dbz_sink_connector.updater_id is
'更新者ID';

comment on column dbz_sink_connector.update_ms is
'更新时间戳';

/*==============================================================*/
/* Index: dbz_sink_connector_PK                                 */
/*==============================================================*/
create unique index dbz_sink_connector_PK on dbz_sink_connector (
                                                                 id
    );

/*==============================================================*/
/* Index: Relationship_1_FK                                     */
/*==============================================================*/
create  index Relationship_1_FK on dbz_sink_connector (
                                                       source_connector_id
    );

/*==============================================================*/
/* Index: Relationship_3_FK                                     */
/*==============================================================*/
create  index Relationship_3_FK on dbz_sink_connector (
                                                       kafka_connect_id
    );

/*==============================================================*/
/* Table: dbz_source_connector                                  */
/*==============================================================*/
create table dbz_source_connector (
                                      id                   INT8                 not null,
                                      kafka_connect_id     INT8                 not null,
                                      name                 VARCHAR(50)          not null,
                                      connector_name       VARCHAR(50)          not null,
                                      connector_config     TEXT                 not null,
                                      run_status           INT2                 not null,
                                      remark               VARCHAR(50)          null,
                                      enabled              BOOL                 not null default true,
                                      creater_id           INT8                 not null,
                                      create_ms            INT8                 not null,
                                      updater_id           INT8                 not null,
                                      update_ms            INT8                 not null,
                                      constraint PK_DBZ_SOURCE_CONNECTOR primary key (id),
                                      constraint AK_NAME_DBZ_SOURCE_CONNECTOR unique (name),
                                      constraint AK_CONNECTOR_NAME_DBZ_SOURCE_CONNECTOR unique (connector_name)
);

comment on table dbz_source_connector is
'上游连接器';

comment on column dbz_source_connector.id is
'ID';

comment on column dbz_source_connector.kafka_connect_id is
'Kafka Connect ID';

comment on column dbz_source_connector.name is
'名称';

comment on column dbz_source_connector.connector_name is
'连接器名称';

comment on column dbz_source_connector.connector_config is
'连接器配置(JSON)';

comment on column dbz_source_connector.run_status is
'运行状态
0-无状态(刚添加记录)
1-运行中
2-已禁用
3-连接KafkaConnect失败
4-注册连接器失败
5-检测连接器失败
6-检测连接器异常
7-检测连接器状态失败
8-检测连接器状态异常';

comment on column dbz_source_connector.remark is
'备注';

comment on column dbz_source_connector.enabled is
'启用';

comment on column dbz_source_connector.creater_id is
'创建者ID';

comment on column dbz_source_connector.create_ms is
'创建时间戳';

comment on column dbz_source_connector.updater_id is
'更新者ID';

comment on column dbz_source_connector.update_ms is
'更新时间戳';

/*==============================================================*/
/* Index: dbz_source_connector_PK                               */
/*==============================================================*/
create unique index dbz_source_connector_PK on dbz_source_connector (
                                                                     id
    );

/*==============================================================*/
/* Index: Relationship_2_FK                                     */
/*==============================================================*/
create  index Relationship_2_FK on dbz_source_connector (
                                                         kafka_connect_id
    );

alter table dbz_sink_connector
    add constraint fk_source_connector_id__from__dbz_source_connector foreign key (source_connector_id)
        references dbz_source_connector (id)
        on delete restrict on update restrict;

alter table dbz_sink_connector
    add constraint fk_kafka_connect_id__from__dbz_kafka_connect foreign key (kafka_connect_id)
        references dbz_kafka_connect (id)
        on delete restrict on update restrict;

alter table dbz_source_connector
    add constraint fk_kafka_connect_id__from__dbz_kafka_connect foreign key (kafka_connect_id)
        references dbz_kafka_connect (id)
        on delete restrict on update restrict;



