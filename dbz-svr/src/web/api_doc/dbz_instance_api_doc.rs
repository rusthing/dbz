use robotech::macros::api_doc;

#[api_doc(
    add,
    modify,
    save,
    del_by_id,
    del_by_query_dto,
    get_by_id,
    get_by_query_dto,
    list_by_query_dto,
    page_by_query_dto,
    register,
    unregister,
    check_status,
    restart
)]
pub struct DbzInstanceApiDoc;
