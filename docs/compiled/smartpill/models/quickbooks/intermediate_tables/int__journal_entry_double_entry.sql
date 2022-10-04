/*
Table that provides the debit and credit records of a journal entry transaction.
*/
with journal_entries as (
    select distinct on (j.id)
        j.*
    from "datawarehouse".dev_quickbooks."journal_entries" j

    left join "datawarehouse".dev_quickbooks."deleted_objects" del on object_type = 'JournalEntry' and j.id = del.id
    where del.id is null or j.updated_at > del.updated_at

    order by j.id, j._airbyte_emitted_at desc
),

journal_entry_lines as (
    select *
    from "datawarehouse".dev_quickbooks."journal_entries_lines"
),

final as (
    select
        journal_entries.id as transaction_id,
        journal_entries.transaction_date,
        -- journal_entry_lines.vendor_id,
        journal_entry_lines.amount * journal_entries.exchange_rate as amount,
        journal_entry_lines.account_id,
        lower(journal_entry_lines.posting_type) as transaction_type,
        'journal_entry' as transaction_source,
        journal_entry_lines.class_id,
        journal_entry_lines.customer_id
    from journal_entries

    inner join journal_entry_lines
        on journal_entries._hash_id = journal_entry_lines._journal_entry_hash_id

    where journal_entry_lines.amount is not null
)

select *
from final