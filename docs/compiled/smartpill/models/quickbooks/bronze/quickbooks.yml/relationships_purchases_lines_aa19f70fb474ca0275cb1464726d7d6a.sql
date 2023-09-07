
    
    

with child as (
    select account_expense_customer_id as from_field
    from "datawarehouse".quickbooks."purchases_lines"
    where account_expense_customer_id is not null
),

parent as (
    select id as to_field
    from "datawarehouse".quickbooks."customers"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


