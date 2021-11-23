
    
    




select count(*) as validation_errors
from (
    select account_id as id from analytics.`quickbook_purchases`
) as child
left join (
    select id as id from analytics.`quickbook_accounts`
) as parent on parent.id = child.id
where child.id is not null
  and parent.id is null


