with purchase_join as (
    with purchases as (
        select * from "datawarehouse".analytics."quickbook_purchases"
    where id in (
        select id
        from "datawarehouse".analytics."quickbook_purchases"
        group by id
        having _airbyte_emitted_at = max(_airbyte_emitted_at)
    )
    ),

    purchase_lines as (
        select *
        from "datawarehouse".analytics."quickbook_purchases_lines"
    ),

    items_stg as (
        select * from "datawarehouse".analytics."quickbook_items"
    where id in (
        select id
        from "datawarehouse".analytics."quickbook_items"
        group by id
        having _airbyte_emitted_at = max(_airbyte_emitted_at)
    )
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
        purchases.id as transaction_id,
        purchases.transaction_date,
        purchase_lines.amount,
        coalesce(purchase_lines.account_expense_account_id, items.parent_expense_account_id, items.expense_account_id) as payed_to_account_id,
        purchases.account_id as payed_from_account_id,
        case when coalesce(purchases.credit, false) = true then 'debit' else 'credit' end as payed_from_transaction_type,
        case when coalesce(purchases.credit, false) = true then 'credit' else 'debit' end as payed_to_transaction_type,
        purchases.currency_name,
        account_expense_class_id as class_id,
        coalesce(purchases.customer_id, purchase_lines.account_expense_customer_id) as customer_id
    from purchases
    
    inner join purchase_lines
        on purchases._hash_id = purchase_lines._purchase_hash_id

    left join items
        on purchase_lines.item_expense_item_id = items.id
),

final as (
    select
        transaction_id,
        transaction_date,
        amount,
        payed_from_account_id as account_id,
        payed_from_transaction_type as transaction_type,
        'purchase' as transaction_source,
        currency_name,
        class_id,
        customer_id
    from purchase_join

    union all

    select
        transaction_id,
        transaction_date,
        amount,
        payed_to_account_id as account_id,
        payed_to_transaction_type as transaction_type,
        'purchase' as transaction_source,
        currency_name,
        class_id,
        customer_id
    from purchase_join
)

select *
from final