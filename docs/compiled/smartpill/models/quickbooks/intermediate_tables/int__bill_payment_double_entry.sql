with bill_payment_join as (
    with bill_payments as (
        select distinct on (bp.id)
            bp.*
        from "datawarehouse".quickbooks."bill_payments" bp
        left join "datawarehouse".quickbooks."deleted_objects" del on object_type = 'BillPayment' and bp.id = del.id
        where del.id is null or bp.updated_at > del.updated_at
        order by bp.id, bp._airbyte_emitted_at desc
    ),

    accounts as (
        select distinct on (id)
            *
        from "datawarehouse".quickbooks."accounts"
        order by id, _airbyte_emitted_at desc
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
        bill_payments.total_amount * bill_payments.exchange_rate as amount,
        coalesce(bill_payments.credit_card_account_id,bill_payments.check_bank_account_id) as payment_account_id,
        ap_accounts.account_id
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
        null as class_id,
        null as customer_id
    from bill_payment_join
)

select *
from final