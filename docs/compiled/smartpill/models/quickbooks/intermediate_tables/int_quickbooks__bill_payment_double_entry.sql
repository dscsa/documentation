with bill_payment_join as (
    with bill_payments as (
        select * from "datawarehouse".prod_analytics."quickbook_bill_payments"
    where id in (
        select id
        from "datawarehouse".prod_analytics."quickbook_bill_payments"
        group by id
        having _airbyte_emitted_at = max(_airbyte_emitted_at)
    )
    ),

    accounts as (
        select * from "datawarehouse".prod_analytics."quickbook_accounts"
    where id in (
        select id
        from "datawarehouse".prod_analytics."quickbook_accounts"
        group by id
        having _airbyte_emitted_at = max(_airbyte_emitted_at)
    )
    ),

    ap_accounts as (
        select
            id as account_id
        from accounts
        
        where account_type = 'Accounts Payable'
            and is_active
    )

    select
        bill_payments.id as transaction_id,
        bill_payments.transaction_date,
        bill_payments.total_amount as amount,
        coalesce(bill_payments.credit_card_account_id,bill_payments.check_bank_account_id) as payment_account_id,
        ap_accounts.account_id,
        bill_payments.currency_name
        -- bill_payments.vendor_id
    from bill_payments

    cross join ap_accounts
),

final as (
    select
        transaction_id,
        transaction_date,
        -- cast(null as int) as customer_id,
        -- vendor_id,
        amount,
        payment_account_id as account_id,
        'credit' as transaction_type,
        'bill payment' as transaction_source,
        currency_name,
        null as class_id,
        null as customer_id
    from bill_payment_join

    union all

    select
        transaction_id,
        transaction_date,
        -- cast(null as int) as customer_id,
        -- vendor_id,
        amount,
        account_id,
        'debit' as transaction_type,
        'bill payment' as transaction_source,
        currency_name,
        null as class_id,
        null as customer_id
    from bill_payment_join
)

select *
from final