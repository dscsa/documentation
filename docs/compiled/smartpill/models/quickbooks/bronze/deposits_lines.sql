select
    _hash_id as _deposit_hash_id,
    _airbyte_emitted_at,
    id as deposit_id,
    jsonb_extract_path_text(_airbyte_nested_data, 'Id') as id,
    cast(jsonb_extract_path_text(_airbyte_nested_data, 'Amount') as decimal) as amount,
    jsonb_extract_path_text(_airbyte_nested_data, 'DepositLineDetail','AccountRef','value') as account_id,
    jsonb_extract_path_text(_airbyte_nested_data, 'DepositLineDetail','ClassRef','value') as class_id,
    jsonb_extract_path_text(_airbyte_nested_data, 'DepositLineDetail','CustomerRef','value') as customer_id,
    jsonb_extract_path_text(_airbyte_nested_data, 'LineNum') as line_num
from "datawarehouse".prod_quickbooks."deposits"
    cross join jsonb_array_elements(line) as _airbyte_nested_data
where
    line is not null

    and _airbyte_emitted_at > (select max(_airbyte_emitted_at) from "datawarehouse".prod_quickbooks."deposits_lines")
