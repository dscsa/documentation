
    
    

with child as (
    select _journal_entry_hash_id as from_field
    from "datawarehouse".dev_quickbooks."journal_entries_lines"
    where _journal_entry_hash_id is not null
),

parent as (
    select _hash_id as to_field
    from "datawarehouse".dev_quickbooks."journal_entries"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


