

select
    _hash_id as _invoice_hash_id,
    id as invoice_id,
    jsonb_extract_path_text(line, 'Id') as id,
    cast(jsonb_extract_path_text(line, 'Amount') as 
    numeric(28, 6)
) as amount,
    jsonb_extract_path_text(line, 'SalesItemLineDetail','ItemAccountRef','value') as sales_item_account_id,
    jsonb_extract_path_text(line, 'SalesItemLineDetail','ItemRef','value') as sales_item_item_id,
    jsonb_extract_path_text(line, 'SalesItemLineDetail','ClassRef','value') as sales_item_class_id,
    jsonb_extract_path_text(line, 'SalesItemLineDetail','Qty') as sales_item_quantity,
    jsonb_extract_path_text(line, 'SalesItemLineDetail','UnitPrice') as sales_item_unit_price,
    jsonb_extract_path_text(line, 'Description') as description,
    jsonb_extract_path_text(line, 'LineNum') as line_num
from "datawarehouse".dev_analytics."quickbook_invoices"

where
    line is not null
    and 
    true
