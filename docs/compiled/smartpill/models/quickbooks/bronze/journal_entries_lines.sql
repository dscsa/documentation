select
    _hash_id as _journal_entry_hash_id,
    _airbyte_emitted_at,
    id as journal_entry_id,
    jsonb_extract_path_text(_airbyte_nested_data, 'Id') as id,
    cast(jsonb_extract_path_text(_airbyte_nested_data, 'Amount') as decimal) as amount,
    jsonb_extract_path_text(_airbyte_nested_data, 'JournalEntryLineDetail','AccountRef','value') as account_id,
    jsonb_extract_path_text(_airbyte_nested_data, 'JournalEntryLineDetail','ClassRef','value') as class_id,
    jsonb_extract_path_text(_airbyte_nested_data, 'JournalEntryLineDetail','CustomerRef','value') as customer_id,
    jsonb_extract_path_text(_airbyte_nested_data, 'JournalEntryLineDetail','PostingType') as posting_type,
    jsonb_extract_path_text(_airbyte_nested_data, 'Description') as description
from "datawarehouse".quickbooks."journal_entries"
cross join jsonb_array_elements(line) as _airbyte_nested_data
where
    line is not null

    and _airbyte_emitted_at > (select max(_airbyte_emitted_at) from "datawarehouse".quickbooks."journal_entries_lines")
