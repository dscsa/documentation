
    
    

with child as (
    select parent_account_id as from_field
    from "datawarehouse".analytics."quickbook_accounts"
    where parent_account_id is not null
),

parent as (
    select id as to_field
    from "datawarehouse".analytics."quickbook_accounts"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


