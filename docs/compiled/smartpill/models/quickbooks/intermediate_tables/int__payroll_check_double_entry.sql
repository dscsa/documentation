with payroll_check as (
    select
        id as transaction_id,
        transaction_date,
        amount,
        account_number,
        account_fully_qualified_name
    from "datawarehouse"."raw".quickbooks_payroll_checks
),

accounts as (
    select distinct on (id)
        id,
        account_number,
        fully_qualified_name
    from "datawarehouse".quickbooks."accounts"
    order by id, _airbyte_emitted_at
),

final as (
    select
        transaction_id,
        transaction_date,
        -- customer_id,
        -- cast(null as int) as vendor_id,
        amount,
        a.id as account_id,
        'debit' as transaction_type,
        'payroll check' as transaction_source,
        null as class_id,
        null as customer_id
    from payroll_check pc
    inner join accounts a on pc.account_number = a.account_number or pc.account_fully_qualified_name = a.fully_qualified_name
)

select *
from final