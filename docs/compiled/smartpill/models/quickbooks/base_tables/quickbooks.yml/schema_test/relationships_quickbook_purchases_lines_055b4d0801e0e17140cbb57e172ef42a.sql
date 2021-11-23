
    
    




select count(*) as validation_errors
from (
    select account_expense_class_id as id from analytics.`quickbook_purchases_lines`
) as child
left join (
    select id as id from analytics.`quickbook_classes`
) as parent on parent.id = child.id
where child.id is not null
  and parent.id is null


