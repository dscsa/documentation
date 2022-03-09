
    
    

with child as (
    select _bill_hash_id as from_field
    from "datawarehouse".prod_analytics."quickbook_bills_lines"
    where _bill_hash_id is not null
),

parent as (
    select _hash_id as to_field
    from "datawarehouse".prod_analytics."quickbook_bills"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


