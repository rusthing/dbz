alter table dbz_kafka_connect
   alter column run_status set default 0;

alter table dbz_sink_connector
   alter column run_status set default 0;

alter table dbz_source_connector
   alter column run_status set default 0;
