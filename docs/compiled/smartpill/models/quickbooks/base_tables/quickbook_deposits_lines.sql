

select
    _hash_id as _deposit_hash_id,
    id as deposit_id,
    jsonb_extract_path_text(line, 'Id') as id,
    cast(jsonb_extract_path_text(line, 'Amount') as 
    numeric(28, 6)
) as amount,
    jsonb_extract_path_text(line, 'DepositLineDetail','AccountRef','value') as account_id,
    jsonb_extract_path_text(line, 'DepositLineDetail','ClassRef','value') as class_id,
    jsonb_extract_path_text(line, 'DepositLineDetail','CustomerRef','value') as customer_id,
    jsonb_extract_path_text(line, 'LineNum') as line_num
from "datawarehouse".prod_analytics."quickbook_deposits"
    
where
    line is not null
    and 
    true
