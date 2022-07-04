with invoice_join as (
    with invoices as (
        select distinct on (id)
            *
        from "datawarehouse".dev_quickbooks."invoices"
        order by id, _airbyte_emitted_at desc
    ),

    invoice_lines as (
        select *
        from "datawarehouse".dev_quickbooks."invoices_lines"
    ),

    items_stg as (
        select distinct on (id)
            *
        from "datawarehouse".dev_quickbooks."items"
        order by id, _airbyte_emitted_at desc
    ),

    items as (
        select
            item.*,
            parent.expense_account_id as parent_expense_account_id,
            parent.income_account_id as parent_income_account_id
        from items_stg item

        left join items_stg parent
            on item.parent_item_id = parent.id

    ),

    accounts as (
        select distinct on (id)
            *
        from "datawarehouse".dev_quickbooks."accounts"
        order by id, _airbyte_emitted_at desc
    )

    select
        invoices.id as transaction_id,
        invoices.transaction_date as transaction_date,
        case when invoices.total_amount != 0
            then invoice_lines.amount
            else invoices.total_amount
                end as amount,

        coalesce(items.income_account_id) as account_id,

        invoices.customer_id,
        invoices.currency_name,
        invoice_lines.sales_item_class_id as class_id

    from invoices

    inner join invoice_lines
        on invoices._hash_id = invoice_lines._invoice_hash_id

    left join items
        on invoice_lines.sales_item_item_id = items.id

    where coalesce(invoice_lines.sales_item_account_id, invoice_lines.sales_item_item_id) is not null

),

ar_accounts as (
    select *
    from "datawarehouse".dev_quickbooks."accounts"

    where account_type = 'Accounts Receivable'
    limit 1
),

final as (
    select
        transaction_id,
        transaction_date,
        -- customer_id,
        -- cast(null as int) as vendor_id,
        amount,
        account_id,
        'credit' as transaction_type,
        'invoice' as transaction_source,
        invoice_join.currency_name,
        class_id,
        customer_id
    from invoice_join

    union all

    select
        transaction_id,
        transaction_date,
        -- customer_id,
        -- cast(null as int) as vendor_id,
        amount,
        ar_accounts.id as account_id,
        'debit' as transaction_type,
        'invoice' as transaction_source,
        invoice_join.currency_name,
        class_id,
        customer_id
    from invoice_join

    cross join ar_accounts
)

select *
from final