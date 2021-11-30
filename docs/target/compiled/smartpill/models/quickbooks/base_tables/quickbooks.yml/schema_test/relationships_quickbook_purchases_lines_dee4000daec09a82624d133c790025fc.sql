
    
    




select count(*) as validation_errors
from (
    select _purchase_hash_id as id from analytics.`quickbook_purchases_lines`
) as child
left join (
    select _hash_id as id from analytics.`quickbook_purchases`
) as parent on parent.id = child.id
where child.id is not null
  and parent.id is null


