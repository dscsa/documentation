
    
    

with child as (
    select _purchase_hash_id as from_field
    from "datawarehouse".quickbooks."purchases_lines"
    where _purchase_hash_id is not null
),

parent as (
    select _hash_id as to_field
    from "datawarehouse".quickbooks."purchases"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


