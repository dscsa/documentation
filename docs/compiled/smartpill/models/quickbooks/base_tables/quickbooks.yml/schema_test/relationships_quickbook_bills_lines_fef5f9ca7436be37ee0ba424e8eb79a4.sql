
    
    




select count(*) as validation_errors
from (
    select _bill_hash_id as id from analytics.`quickbook_bills_lines`
) as child
left join (
    select _hash_id as id from analytics.`quickbook_bills`
) as parent on parent.id = child.id
where child.id is not null
  and parent.id is null


