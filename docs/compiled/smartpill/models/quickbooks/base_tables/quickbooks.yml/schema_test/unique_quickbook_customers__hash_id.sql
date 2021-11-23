
    
    



select count(*) as validation_errors
from (

    select
        _hash_id

    from analytics.`quickbook_customers`
    where _hash_id is not null
    group by _hash_id
    having count(*) > 1

) validation_errors


