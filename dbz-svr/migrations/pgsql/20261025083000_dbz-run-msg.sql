alter table dbz_kafka_broker
   add column run_msg VARCHAR(500) null;

comment on column dbz_kafka_broker.run_msg is
'检查结论（最近一次检查的失败原因，成功时为空）
@len 0..=500';

alter table dbz_kafka_connect
   add column run_msg VARCHAR(500) null;

comment on column dbz_kafka_connect.run_msg is
'检查结论（最近一次检查的失败原因，成功时为空）
@len 0..=500';

alter table dbz_sink_connector
   add column run_msg VARCHAR(500) null;

comment on column dbz_sink_connector.run_msg is
'检查结论（最近一次检查的失败原因，成功时为空）
@len 0..=500';

alter table dbz_source_connector
   add column run_msg VARCHAR(500) null;

comment on column dbz_source_connector.run_msg is
'检查结论（最近一次检查的失败原因，成功时为空）
@len 0..=500';
