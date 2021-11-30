with payroll_check as (
    select
        id as transaction_id,
        transaction_date,
        total_amount as amount,
        deposit_to_account_id
    from analytics_v2.quickbook_payroll_checks
),

final as (
    select
        transaction_id,
        transaction_date,
        -- customer_id,
        -- cast(null as int) as vendor_id,
        amount,
        deposit_to_account_id as account_id,
        'debit' as transaction_type,
        'payroll check' as transaction_source,
        'United States Dollar',
        null as class_id,
        null as customer_id
    from payroll_check
)

select *
from final