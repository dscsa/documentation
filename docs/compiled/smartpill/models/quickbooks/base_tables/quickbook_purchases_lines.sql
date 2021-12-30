

select
    _hash_id as _purchase_hash_id,
    id as purchase_id,
    jsonb_extract_path_text(line, 'Id') as id,
    jsonb_extract_path_text(line, 'AccountBasedExpenseLineDetail','AccountRef','value') as account_expense_account_id,
    jsonb_extract_path_text(line, 'AccountBasedExpenseLineDetail','ClassRef','value') as account_expense_class_id,
    jsonb_extract_path_text(line, 'AccountBasedExpenseLineDetail','CustomerRef','value') as account_expense_customer_id,
    jsonb_extract_path_text(line, 'ItemBasedExpenseLineDetail','ItemRef','value') as item_expense_item_id,
    jsonb_extract_path_text(line, 'ItemBasedExpenseLineDetail','BillableStatus') as item_expense_billable_status,
    jsonb_extract_path_text(line, 'Description') as description,
    cast(jsonb_extract_path_text(line, 'Amount') as 
    numeric(28, 6)
) as amount
from "datawarehouse".analytics."quickbook_purchases"

where
    line is not null
    and 
    true
