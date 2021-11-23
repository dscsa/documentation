/*
Table that provides the debit and credit records of a journal entry transaction.
*/
with journal_entries as (
    select * from analytics.`quickbook_journal_entries`
    where id in (
        select id
        from analytics.`quickbook_journal_entries`
        group by id
        having _airbyte_emitted_at = max(_airbyte_emitted_at)
    )
),

journal_entry_lines as (
    select *
    from analytics.`quickbook_journal_entries_lines`
),

final as (
    select
        journal_entries.id as transaction_id,
        journal_entries.transaction_date,
        -- journal_entry_lines.vendor_id,
        journal_entry_lines.amount,
        journal_entry_lines.account_id,
        lower(journal_entry_lines.posting_type) as transaction_type,
        'journal_entry' as transaction_source,
        journal_entries.currency_name,
        journal_entry_lines.class_id,
        journal_entry_lines.customer_id
    from journal_entries

    inner join journal_entry_lines
        on journal_entries._hash_id = journal_entry_lines._journal_entry_hash_id

    where journal_entry_lines.amount is not null
)

select *
from final