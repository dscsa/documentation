
    
    



select count(*) as validation_errors
from analytics.`quickbook_bills`
where created_at is null


