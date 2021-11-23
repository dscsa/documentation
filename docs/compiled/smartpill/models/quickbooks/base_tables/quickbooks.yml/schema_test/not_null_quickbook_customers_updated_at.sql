
    
    



select count(*) as validation_errors
from analytics.`quickbook_customers`
where updated_at is null


