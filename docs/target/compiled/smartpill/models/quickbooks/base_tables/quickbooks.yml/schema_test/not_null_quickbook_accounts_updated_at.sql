
    
    



select count(*) as validation_errors
from analytics.`quickbook_accounts`
where updated_at is null


