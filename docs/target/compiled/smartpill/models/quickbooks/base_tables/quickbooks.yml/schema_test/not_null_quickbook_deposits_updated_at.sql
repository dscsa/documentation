
    
    



select count(*) as validation_errors
from analytics.`quickbook_deposits`
where updated_at is null


