
    
    

with child as (
    select customer_id as from_field
    from "datawarehouse".prod_quickbooks."deposits_lines"
    where customer_id is not null
),

parent as (
    select id as to_field
    from "datawarehouse".prod_quickbooks."customers"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


