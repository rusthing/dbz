use robotech::macros::dao;

#[dao(
    unique_keys: [
        ("name", "任务名称"),
    ],
    like_columns: [
        Column::Name,
        Column::ConnectorName,
        Column::Remark
    ],
)]
pub struct DbzTaskDao;