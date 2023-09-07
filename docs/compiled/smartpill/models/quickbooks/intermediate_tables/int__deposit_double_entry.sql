with deposit_join as (
    with deposits as (
        select distinct on (d.id)
            d.*
        from "datawarehouse".quickbooks."deposits" d
        left join "datawarehouse".quickbooks."deleted_objects" del on object_type = 'Deposit' and d.id = del.id
        where del.id is null or d.updated_at > del.updated_at
        order by id, _airbyte_emitted_at desc
    ),

    deposit_lines as (
        select *
        from "datawarehouse".quickbooks."deposits_lines"
    ),

    accounts as (
        select distinct on (id)
            *
        from "datawarehouse".quickbooks."accounts"
        order by id, _airbyte_emitted_at desc
    ),

    uf_accounts as (
        select
            id
        from accounts

        where account_sub_type = 'UndepositedFunds'
            and is_active
    )

    select
        deposits.id as transaction_id,
        deposits.transaction_date,
        deposit_lines.amount * deposits.exchange_rate as amount,
        deposits.account_id as deposit_to_acct_id,
        coalesce(deposit_lines.account_id, uf_accounts.id) as deposit_from_acct_id,
        customer_id as customer_id,
        deposit_lines.class_id
    from deposits

    inner join deposit_lines
        on deposits._hash_id = deposit_lines._deposit_hash_id

    cross join uf_accounts
),

final as (
    select
        transaction_id,
        transaction_date,
        -- customer_id,
        -- cast(null as int) as vendor_id,
        amount,
        deposit_to_acct_id as account_id,
        'debit' as transaction_type,
        'deposit' as transaction_source,
        class_id,
        customer_id
    from deposit_join

    union all

    select
        transaction_id,
        transaction_date,
        -- customer_id,
        -- cast(null as int) as vendor_id,
        amount,
        deposit_from_acct_id as account_id,
        'credit' as transaction_type,
        'deposit' as transaction_source,
        class_id,
        customer_id
    from deposit_join
)

select *
from final