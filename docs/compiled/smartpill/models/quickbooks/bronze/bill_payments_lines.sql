with bill_payment as (
    select
        _hash_id as _bill_payment_hash_id,
        _airbyte_emitted_at,
        id as bill_payment_id,
		jsonb_extract_path_text(line, 'Id') as id,
		cast(jsonb_extract_path_text(line, 'Amount') as decimal) as amount,
		trim(both '"' from cast(line->'LinkedTxn'->0->'TxnId' as text)) as transaction_id,
		cast(line->'linkedtxn'->0 as text) as transaction_type
    from "datawarehouse".dev_quickbooks."bill_payments"
    cross join jsonb_array_elements(line) as line
    where 
        line is not null
    
        and _airbyte_emitted_at > (select max(_airbyte_emitted_at) from "datawarehouse".dev_quickbooks."bill_payments_lines")
    
)
select
    *,
	case when transaction_type = 'Bill' then transaction_id else null end as bill_id,
	case when transaction_type = 'Deposit' then transaction_id else null end as deposit_id,
	case when transaction_type = 'JournalEntry' then transaction_id else null end as journal_entry_id,
	case when transaction_type = 'Expense' then transaction_id else null end as expense_id
from bill_payment