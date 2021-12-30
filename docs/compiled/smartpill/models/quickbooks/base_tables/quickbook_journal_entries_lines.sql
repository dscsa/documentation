


select
    _hash_id as _journal_entry_hash_id,
    id as journal_entry_id,
    jsonb_extract_path_text(line, 'Id') as id,
    cast(jsonb_extract_path_text(line, 'Amount') as 
    numeric(28, 6)
) as amount,
    jsonb_extract_path_text(line, 'JournalEntryLineDetail','AccountRef','value') as account_id,
    jsonb_extract_path_text(line, 'JournalEntryLineDetail','ClassRef','value') as class_id,
    jsonb_extract_path_text(line, 'JournalEntryLineDetail','CustomerRef','value') as customer_id,
    jsonb_extract_path_text(line, 'JournalEntryLineDetail','PostingType') as posting_type,
    jsonb_extract_path_text(line, 'Description') as description
from "datawarehouse".analytics."quickbook_journal_entries"

where
    line is not null
    and 
    true
