select
    _hash_id as _bill_hash_id,
    _airbyte_emitted_at,
    id as bill_id,
    jsonb_extract_path_text(_airbyte_nested_data, 'Id') as id,
    jsonb_extract_path_text(_airbyte_nested_data, 'AccountBasedExpenseLineDetail','AccountRef','value') as account_expense_account_id,
    jsonb_extract_path_text(_airbyte_nested_data, 'AccountBasedExpenseLineDetail','ClassRef','value') as account_expense_class_id,
    jsonb_extract_path_text(_airbyte_nested_data, 'AccountBasedExpenseLineDetail','CustomerRef','value') as account_expense_customer_id,
    jsonb_extract_path_text(_airbyte_nested_data, 'AccountBasedExpenseLineDetail','BillableStatus') as account_expense_billable_status,
    jsonb_extract_path_text(_airbyte_nested_data, 'ItemBasedExpenseLineDetail','ClassRef','value') as item_expense_class_id,
    jsonb_extract_path_text(_airbyte_nested_data, 'ItemBasedExpenseLineDetail','CustomerRef','value') as item_expense_customer_id,
    jsonb_extract_path_text(_airbyte_nested_data, 'ItemBasedExpenseLineDetail','BillableStatus') as item_expense_billable_status,
    jsonb_extract_path_text(_airbyte_nested_data, 'ItemBasedExpenseLineDetail','ItemRef','value') as item_expense_item_id,
    jsonb_extract_path_text(_airbyte_nested_data, 'Description') as description,
    cast(jsonb_extract_path_text(_airbyte_nested_data, 'Amount') as decimal) as amount
from "datawarehouse".dev_quickbooks."bills"
cross join jsonb_array_elements(line) as _airbyte_nested_data
where
    line is not null

    and _airbyte_emitted_at > (select max(_airbyte_emitted_at) from "datawarehouse".dev_quickbooks."bills_lines")
