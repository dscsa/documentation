
,
bill_payment as (
    select
        _hash_id as _bill_payment_hash_id,
        id as bill_payment_id,
        json_value(line, '$."LinkedTxn"[0]."TxnId"') as transaction_id,
        json_value(line, '$."LinkedTxn"[0]"."TxnType"') as transaction_type,
        cast(jsonb_extract_path_text(line, 'Amount') as 
    numeric(28, 6)
) as amount
    from "datawarehouse".dev_analytics."quickbook_bill_payments"
    
    where
        line is not null
        and 
    true

)
select
    *,
    if(transaction_type = 'Bill', transaction_id, null) as bill_id,
    if(transaction_type = 'Deposit', transaction_id, null) as deposit_id,
    if(transaction_type = 'JournalEntry', transaction_id, null) as journal_entry_id,
    if(transaction_type = 'Expense', transaction_id, null) as expense_id
from bill_payment