with transfers as (
    select distinct on (id)
        *
    from "datawarehouse".dev_quickbooks."transfers"
    order by id, _airbyte_emitted_at desc
),

transfer_body as (
    select
        id as transaction_id,
        transaction_date,
        amount,
        currency_name,
        from_account_id as credit_to_account_id,
        to_account_id as debit_to_account_id
    from transfers
),

final as (
    select 
        transaction_id,
        transaction_date,
        amount,
        credit_to_account_id as account_id,
        'credit' as transaction_type,
        'transfer' as transaction_source,
        currency_name,
        null::varchar as class_id,
        null::varchar as customer_id
    from transfer_body

    union all

    select 
        transaction_id,
        transaction_date,
        amount,
        debit_to_account_id as account_id,
        'debit' as transaction_type,
        'transfer' as transaction_source,
        currency_name,
        null::varchar as class_id,
        null::varchar as customer_id
    from transfer_body
)

select *
from final