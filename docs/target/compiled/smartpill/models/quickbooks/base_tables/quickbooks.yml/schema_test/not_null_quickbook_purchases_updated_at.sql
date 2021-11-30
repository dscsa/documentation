
    
    



select count(*) as validation_errors
from analytics.`quickbook_purchases`
where updated_at is null


