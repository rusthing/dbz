use robotech::macros::api_doc;

#[api_doc(
    add,
    modify,
    save,
    del_by_id,
    del_by_query_dto,
    get_by_id,
    get_ex_by_id,
    get_by_query_dto,
    get_ex_by_query_dto,
    list_by_query_dto,
    list_ex_by_query_dto,
    page_by_query_dto,
    page_ex_by_query_dto,
    search_by_id,
    search_by_condition,
    search_list,
    search_page,
    search_ex_by_id,
    search_ex_by_condition,
    search_list_ex,
    search_page_ex
)]
pub struct DbzKafkaBrokerApiDoc;
