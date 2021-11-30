
    
    



select count(*) as validation_errors
from analytics.`quickbook_bill_payments`
where updated_at is null


