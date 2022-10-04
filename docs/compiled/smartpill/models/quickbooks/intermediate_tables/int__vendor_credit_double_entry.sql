with vendor_credits as (
    select distinct on (v.id)
        v.*
    from "datawarehouse".dev_quickbooks."vendor_credits" v

    left join "datawarehouse".dev_quickbooks."deleted_objects" del on object_type = 'VendorCredit' and v.id = del.id
    where del.id is null or v.updated_at > del.updated_at

    order by v.id, v._airbyte_emitted_at desc
),

vendor_credit_lines as (
    select *
    from "datawarehouse".dev_quickbooks."vendor_credits_lines"
),

vendor_credit_join as (
    select
        vendor_credits.id as transaction_id,
        vendor_credits.transaction_date,
        vendor_credit_lines.amount * vendor_credits.exchange_rate as amount,
        vendor_credit_lines.account_expense_class_id as class_id,
        vendor_credits.payable_account_id as debit_to_account_id,
        vendor_credit_lines.account_expense_account_id as credit_account_id,
        account_expense_customer_id as customer_id,
        vendor_credits.vendor_id
    from vendor_credits
    
    inner join vendor_credit_lines 
        on vendor_credits._hash_id = vendor_credit_lines._vendor_credit_hash_id
),

final as (
    select 
        transaction_id,
        transaction_date,
        amount,
        credit_account_id as account_id,
        'credit' as transaction_type,
        'vendor_credit' as transaction_source,
        class_id,
        customer_id
    from vendor_credit_join

    union all

    select 
        transaction_id,
        transaction_date,
        amount,
        debit_to_account_id as account_id,
        'debit' as transaction_type,
        'vendor_credit' as transaction_source,
        class_id,
        customer_id
    from vendor_credit_join
)

select *
from final