with payment as (
    select
        id as transaction_id,
        transaction_date,
        total_amount as amount,
        deposit_to_account_id,
        receivable_account_id,
        customer_id as customer_id,
        currency_name
    from "datawarehouse".prod_analytics."quickbook_payments"
    where id in (
        select id
        from "datawarehouse".prod_analytics."quickbook_payments"
        group by id
        having _airbyte_emitted_at = max(_airbyte_emitted_at)
    )
),

ar_accounts as (
    select
        id
    from "datawarehouse".prod_analytics."quickbook_accounts"

    where account_type = 'Accounts Receivable'
        and is_active
    limit 1
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
        'payment' as transaction_source,
        payment.currency_name,
        null as class_id,
        customer_id
    from payment

    union all

    select
        transaction_id,
        transaction_date,
        -- customer_id,
        -- cast(null as int) as vendor_id,
        amount,
        coalesce(receivable_account_id, ar_accounts.id) as account_id,
        'credit' as transaction_type,
        'payment' as transaction_source,
        payment.currency_name,
        null as class_id,
        customer_id
    from payment
    
    cross join ar_accounts
)

select *
from final