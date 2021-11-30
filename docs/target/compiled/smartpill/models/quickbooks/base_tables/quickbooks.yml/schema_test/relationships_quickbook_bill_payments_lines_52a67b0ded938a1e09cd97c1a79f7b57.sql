
    
    




select count(*) as validation_errors
from (
    select journal_entry_id as id from analytics.`quickbook_bill_payments_lines`
) as child
left join (
    select id as id from analytics.`quickbook_journal_entries`
) as parent on parent.id = child.id
where child.id is not null
  and parent.id is null


