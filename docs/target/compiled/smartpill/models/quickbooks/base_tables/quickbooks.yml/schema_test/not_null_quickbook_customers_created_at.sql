
    
    



select count(*) as validation_errors
from analytics.`quickbook_customers`
where created_at is null


