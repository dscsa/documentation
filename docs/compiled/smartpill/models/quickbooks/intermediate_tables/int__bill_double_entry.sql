with bill_join as (
    with bills as (
        select distinct on (id)
            *
        from "datawarehouse".dev_quickbooks."bills"
        order by id, _airbyte_emitted_at desc
    ),

    bill_lines as (
        select *
        from "datawarehouse".dev_quickbooks."bills_lines"
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

    )

    select
        bills.id as transaction_id,
        bills.transaction_date,
        bill_lines.amount,
        coalesce(bill_lines.account_expense_account_id, items.expense_account_id, items.parent_expense_account_id, items.expense_account_id, items.parent_income_account_id, items.income_account_id) as payed_to_account_id,
        bills.payable_account_id,
        coalesce(bill_lines.account_expense_customer_id, bill_lines.item_expense_customer_id) as customer_id,
        bills.currency_name,
        coalesce(bill_lines.account_expense_class_id, bill_lines.item_expense_class_id) as class_id
        -- bills.vendor_id
    from bills

    inner join bill_lines
        on bills._hash_id = bill_lines._bill_hash_id

    left join items
        on bill_lines.item_expense_item_id = items.id
),

final as (
    select
        transaction_id,
        transaction_date,
        -- vendor_id,
        amount,
        payed_to_account_id as account_id,
        'debit' as transaction_type,
        'bill' as transaction_source,
        currency_name,
        class_id,
        customer_id
    from bill_join

    union all

    select
        transaction_id,
        transaction_date,
        -- vendor_id,
        amount,
        payable_account_id as account_id,
        'credit' as transaction_type,
        'bill' as transaction_source,
        currency_name,
        class_id,
        customer_id
    from bill_join
)

select *
from final