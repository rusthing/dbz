use robotech::macros::dao;

#[dao(
    unique_keys: [
        ("name", "实例名称"),
        ("job_id", "关联的任务计划ID"),
        ("source_connector_name", "上游连接器名称"),
        ("sink_connector_name", "下游连接器名称"),
    ],
    like_columns: [
        Column::Name,
        Column::SourceConnectorName,
        Column::SinkConnectorName,
        Column::Remark,
    ],
)]
pub struct DbzInstanceDao;