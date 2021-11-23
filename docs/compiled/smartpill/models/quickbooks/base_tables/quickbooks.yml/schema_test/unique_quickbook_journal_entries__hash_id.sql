
    
    



select count(*) as validation_errors
from (

    select
        _hash_id

    from analytics.`quickbook_journal_entries`
    where _hash_id is not null
    group by _hash_id
    having count(*) > 1

) validation_errors


