
    
    



select count(*) as validation_errors
from analytics.`quickbook_journal_entries`
where created_at is null


