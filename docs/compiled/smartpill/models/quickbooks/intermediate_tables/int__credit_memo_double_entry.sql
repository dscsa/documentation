with credit_memos as (
    select distinct on (c.id)
        c.*
    from "datawarehouse".prod_quickbooks."credit_memos" c
    where id not in (select id from "datawarehouse".prod_quickbooks."deleted_objects" del where object_type = 'CreditMemo' and c.updated_at <= del.updated_at)
    order by c.id, c._airbyte_emitted_at desc
),

credit_memo_lines as (
    select *
    from "datawarehouse".prod_quickbooks."credit_memos_lines"
),

items as (
    select distinct on (i.id) 
        i.*
    from "datawarehouse".prod_quickbooks."items" i 
    left join "datawarehouse".prod_quickbooks."deleted_objects" del on object_type = 'Item' and i.id = del.id
    where del.id is null or i.updated_at > del.updated_at
    order by i.id, i._airbyte_emitted_at desc
),

accounts as (
    select distinct on (id)
        *
    from "datawarehouse".prod_quickbooks."accounts"
    order by id, _airbyte_emitted_at desc
),

df_accounts as (
    select
        id as account_id
    from accounts

    where account_type = 'Accounts Receivable'
        and is_active
),

credit_memo_join as (
    select
        credit_memos.id as transaction_id,
        credit_memos.transaction_date,
        credit_memo_lines.amount * credit_memos.exchange_rate as amount,
        coalesce(credit_memo_lines.sales_item_account_id, items.income_account_id, items.expense_account_id) as account_id,
        credit_memos.customer_id,
        credit_memo_lines.sales_item_class_id as class_id
    from credit_memos

    inner join credit_memo_lines
        on credit_memos._hash_id = credit_memo_lines._credit_memo_hash_id

    left join items
        on credit_memo_lines.sales_item_item_id = items.id

    where coalesce(credit_memo_lines.sales_item_account_id, credit_memo_lines.sales_item_item_id) is not null
),

final as (
    select
        transaction_id,
        transaction_date,
        amount * -1 as amount,
        account_id,
        'credit' as transaction_type,
        'credit_memo' as transaction_source,
        class_id,
        customer_id
    from credit_memo_join

    union all

    select 
        transaction_id,
        transaction_date,
        amount * -1 as amount,
        df_accounts.account_id,
        'debit' as transaction_type,
        'credit_memo' as transaction_source,
        class_id,
        customer_id
    from credit_memo_join

    cross join df_accounts
)

select *
from final