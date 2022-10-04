with transfers as (
    select distinct on (t.id)
        t.*
    from "datawarehouse".prod_quickbooks."transfers" t

    left join "datawarehouse".prod_quickbooks."deleted_objects" del on object_type = 'Transfer' and t.id = del.id
    where del.id is null or t.updated_at > del.updated_at

    order by t.id, t._airbyte_emitted_at desc
),

transfer_body as (
    select
        id as transaction_id,
        transaction_date,
        amount * exchange_rate as amount,
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
        null::varchar as class_id,
        null::varchar as customer_id
    from transfer_body
)

select *
from final