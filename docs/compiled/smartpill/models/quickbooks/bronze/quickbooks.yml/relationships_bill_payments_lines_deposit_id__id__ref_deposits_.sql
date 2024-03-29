
    
    

with child as (
    select deposit_id as from_field
    from "datawarehouse".quickbooks."bill_payments_lines"
    where deposit_id is not null
),

parent as (
    select id as to_field
    from "datawarehouse".quickbooks."deposits"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


