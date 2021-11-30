
    
    




select count(*) as validation_errors
from (
    select bill_id as id from analytics.`quickbook_bills_lines`
) as child
left join (
    select id as id from analytics.`quickbook_bills`
) as parent on parent.id = child.id
where child.id is not null
  and parent.id is null


