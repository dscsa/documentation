
    
    



select count(*) as validation_errors
from analytics.`quickbook_journal_entries`
where updated_at is null


