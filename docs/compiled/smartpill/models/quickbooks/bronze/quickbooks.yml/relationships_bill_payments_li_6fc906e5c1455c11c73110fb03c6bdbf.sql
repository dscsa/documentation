
    
    

with child as (
    select journal_entry_id as from_field
    from "datawarehouse".dev_quickbooks."bill_payments_lines"
    where journal_entry_id is not null
),

parent as (
    select id as to_field
    from "datawarehouse".dev_quickbooks."journal_entries"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


