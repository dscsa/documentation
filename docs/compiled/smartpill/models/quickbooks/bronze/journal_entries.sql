with final as (
    select
        jsonb_extract_path_text(_airbyte_data, 'Id') as id,
        cast(jsonb_extract_path_text(_airbyte_data, 'MetaData','CreateTime') as timestamp) as created_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'MetaData','LastUpdatedTime') as timestamp) as updated_at,
        _airbyte_emitted_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'Adjustment') as bool) as is_adjustment,
        jsonb_extract_path_text(_airbyte_data, 'CurrencyRef','name') as currency_name,
        jsonb_extract_path(_airbyte_data, 'Line') as line,
        jsonb_extract_path_text(_airbyte_data, 'PrivateNote') as private_note,
        cast(jsonb_extract_path_text(_airbyte_data, 'TxnDate') as timestamp) as transaction_date
    from
        "datawarehouse"."raw"._airbyte_raw_quickbooks_journal_entries
)
select
    *,
    md5("id" || '-' || "_airbyte_emitted_at") as _hash_id
from
    final

where
    _airbyte_emitted_at > (select max(_airbyte_emitted_at) from "datawarehouse".dev_quickbooks."journal_entries")
