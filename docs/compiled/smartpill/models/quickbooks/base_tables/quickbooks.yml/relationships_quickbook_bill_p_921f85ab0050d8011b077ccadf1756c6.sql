
    
    

with child as (
    select bill_id as from_field
    from "datawarehouse".analytics."quickbook_bill_payments_lines"
    where bill_id is not null
),

parent as (
    select id as to_field
    from "datawarehouse".analytics."quickbook_bills"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


