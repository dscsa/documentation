
    
    




select count(*) as validation_errors
from (
    select customer_id as id from analytics.`quickbook_deposits_lines`
) as child
left join (
    select id as id from analytics.`quickbook_customers`
) as parent on parent.id = child.id
where child.id is not null
  and parent.id is null


