
    
    



select count(*) as validation_errors
from analytics.`quickbook_bill_payments`
where created_at is null


