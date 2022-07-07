select
    _hash_id as _invoice_hash_id,
    id as invoice_id,
    jsonb_extract_path_text(_airbyte_nested_data, 'Id') as id,
    cast(jsonb_extract_path_text(_airbyte_nested_data, 'Amount') as decimal) as amount,
    jsonb_extract_path_text(_airbyte_nested_data, 'SalesItemLineDetail','ItemAccountRef','value') as sales_item_account_id,
    jsonb_extract_path_text(_airbyte_nested_data, 'SalesItemLineDetail','ItemRef','value') as sales_item_item_id,
    jsonb_extract_path_text(_airbyte_nested_data, 'SalesItemLineDetail','ClassRef','value') as sales_item_class_id,
    jsonb_extract_path_text(_airbyte_nested_data, 'SalesItemLineDetail','Qty') as sales_item_quantity,
    jsonb_extract_path_text(_airbyte_nested_data, 'SalesItemLineDetail','UnitPrice') as sales_item_unit_price,
    jsonb_extract_path_text(_airbyte_nested_data, 'Description') as description,
    jsonb_extract_path_text(_airbyte_nested_data, 'LineNum') as line_num
from "datawarehouse".dev_quickbooks."invoices"
cross join jsonb_array_elements(line) as _airbyte_nested_data
where
    line is not null

    and _airbyte_emitted_at > (select max(_airbyte_emitted_at) from "datawarehouse".dev_quickbooks."invoices")
