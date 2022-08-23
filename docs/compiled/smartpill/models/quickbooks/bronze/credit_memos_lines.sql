select
    _hash_id as _credit_memo_hash_id,
    id as purchase_id,
    jsonb_extract_path_text(_airbyte_nested_data, 'Id') as id,
    jsonb_extract_path_text(_airbyte_nested_data, 'SalesItemLineDetail','ItemRef','value') as sales_item_item_id,
    jsonb_extract_path_text(_airbyte_nested_data, 'SalesItemLineDetail','AccountRef','value') as sales_item_account_id,
    jsonb_extract_path_text(_airbyte_nested_data, 'SalesItemLineDetail','ClassRef','value') as sales_item_class_id,
    cast(jsonb_extract_path_text(_airbyte_nested_data, 'Amount') as decimal) as amount
from "datawarehouse".dev_quickbooks."credit_memos"
cross join jsonb_array_elements(line) as _airbyte_nested_data
where
    line is not null
    and jsonb_extract_path_text(_airbyte_nested_data, 'Id') is not null

    and _airbyte_emitted_at > (select max(_airbyte_emitted_at) from "datawarehouse".dev_quickbooks."credit_memos")