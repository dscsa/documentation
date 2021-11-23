
    
    



select count(*) as validation_errors
from analytics.`quickbook_bills`
where updated_at is null


