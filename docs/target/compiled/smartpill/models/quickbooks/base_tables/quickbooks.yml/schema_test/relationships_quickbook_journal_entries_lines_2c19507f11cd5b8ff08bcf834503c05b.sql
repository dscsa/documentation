
    
    




select count(*) as validation_errors
from (
    select _journal_entry_hash_id as id from analytics.`quickbook_journal_entries_lines`
) as child
left join (
    select _hash_id as id from analytics.`quickbook_journal_entries`
) as parent on parent.id = child.id
where child.id is not null
  and parent.id is null


