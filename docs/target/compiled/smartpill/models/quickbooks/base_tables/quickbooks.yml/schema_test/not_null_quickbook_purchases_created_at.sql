
    
    



select count(*) as validation_errors
from analytics.`quickbook_purchases`
where created_at is null


