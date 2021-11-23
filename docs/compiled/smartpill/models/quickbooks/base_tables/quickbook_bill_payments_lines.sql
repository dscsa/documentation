
with numbers as (
        

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * pow(2, 0)
     + 
    
    p1.generated_number * pow(2, 1)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
    
    

    )

    select *
    from unioned
    where generated_number <= 4
    order by generated_number


    ),
    joined as (
        select
            _hash_id as _airbyte_hashid,
            
            json_extract(line, concat("$[", numbers.generated_number - 1, "][0]")) as _airbyte_nested_data
        from analytics.`quickbook_bill_payments`
        cross join numbers
        -- only generate the number of records in the cross join that corresponds
        -- to the number of items in quickbook_bill_payments.line
        where numbers.generated_number <= json_length(line)
    ),
bill_payment as (
    select
        _hash_id as _bill_payment_hash_id,
        id as bill_payment_id,
        json_value(_airbyte_nested_data, '$."LinkedTxn"[0]."TxnId"') as transaction_id,
        json_value(_airbyte_nested_data, '$."LinkedTxn"[0]"."TxnType"') as transaction_type,
        cast(json_value(_airbyte_nested_data, 
    '$."Amount"') as 
    float
) as amount
    from analytics.`quickbook_bill_payments`
    left join joined on _hash_id = joined._airbyte_hashid
    where 
        line is not null
        and 
    _airbyte_emitted_at >= (select max(_airbyte_emitted_at) from analytics.`quickbook_bill_payments`)

)
select
    *,
    if(transaction_type = 'Bill', transaction_id, null) as bill_id,
    if(transaction_type = 'Deposit', transaction_id, null) as deposit_id,
    if(transaction_type = 'JournalEntry', transaction_id, null) as journal_entry_id,
    if(transaction_type = 'Expense', transaction_id, null) as expense_id
from bill_payment