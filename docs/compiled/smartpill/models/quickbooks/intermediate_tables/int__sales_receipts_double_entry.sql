with sales_receipts as (
    select distinct on (id)
        *
    from "datawarehouse".dev_quickbooks."sales_receipts"
    order by id, _airbyte_emitted_at desc
),

sales_receipt_lines as (
    select
        *
    from "datawarehouse".dev_quickbooks."sales_receipts_lines"
),

items as (
    select distinct on (item.id)
        item.*,
        parent.income_account_id as parent_income_account_id
    from "datawarehouse".dev_quickbooks."items" item
    left join "datawarehouse".dev_quickbooks."items" parent
        on item.parent_item_id = parent.id
    order by id, _airbyte_emitted_at desc
),

sales_receipt_join as (
    select
        sales_receipts.id as transaction_id,
        sales_receipts.transaction_date,
        sales_receipt_lines.amount,
        sales_receipts.deposit_to_account_id as debit_to_account_id,
        coalesce(items.parent_income_account_id, items.income_account_id) as credit_to_account_id,
        sales_receipts.customer_id,
        sales_receipts.currency_name,
        sales_receipt_lines.sales_item_class_id as class_id
    from sales_receipts

    inner join sales_receipt_lines
        on sales_receipts._hash_id = sales_receipt_lines._sales_receipts_hash_id

    left join items
        on sales_receipt_lines.sales_item_item_id = items.id

    where coalesce(sales_receipt_lines.sales_item_item_id) is not null
),

final as (
    select
        transaction_id,
        transaction_date,
        amount,
        debit_to_account_id as account_id,
        'debit' as transaction_type,
        'sales_receipt' as transaction_source,
        currency_name,
        class_id,
        customer_id
    from sales_receipt_join

    union all

    select
        transaction_id,
        transaction_date,
        amount,
        credit_to_account_id as account_id,
        'credit' as transaction_type,
        'sales_receipt' as transaction_source,
        currency_name,
        class_id,
        customer_id
    from sales_receipt_join
)

select *
from final