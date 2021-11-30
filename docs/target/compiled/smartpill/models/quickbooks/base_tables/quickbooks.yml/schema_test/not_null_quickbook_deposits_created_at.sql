
    
    



select count(*) as validation_errors
from analytics.`quickbook_deposits`
where created_at is null


