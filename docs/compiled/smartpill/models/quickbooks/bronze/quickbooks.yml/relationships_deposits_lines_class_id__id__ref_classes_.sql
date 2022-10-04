
    
    

with child as (
    select class_id as from_field
    from "datawarehouse".dev_quickbooks."deposits_lines"
    where class_id is not null
),

parent as (
    select id as to_field
    from "datawarehouse".dev_quickbooks."classes"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


