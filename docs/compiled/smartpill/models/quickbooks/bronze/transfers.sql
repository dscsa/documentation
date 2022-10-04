with final as (
    select
        jsonb_extract_path_text(_airbyte_data, 'Id') as id,
        cast(jsonb_extract_path_text(_airbyte_data, 'MetaData','CreateTime') as timestamp) as created_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'MetaData','LastUpdatedTime') as timestamp) as updated_at,
        _airbyte_emitted_at,
        jsonb_extract_path_text(_airbyte_data, 'CurrencyRef','name') as currency_name,
        cast(jsonb_extract_path_text(_airbyte_data, 'ExchangeRate') as decimal) as exchange_rate,
        jsonb_extract_path_text(_airbyte_data, 'ToAccountRef', 'value') as to_account_id,
        jsonb_extract_path_text(_airbyte_data, 'FromAccountRef', 'value') as from_account_id,
        jsonb_extract_path_text(_airbyte_data, 'PrivateNote') as private_note,
        cast(jsonb_extract_path_text(_airbyte_data, 'Amount') as decimal) as amount,
        cast(jsonb_extract_path_text(_airbyte_data, 'TxnDate') as timestamp) as transaction_date
    from
        "datawarehouse"."raw"._airbyte_raw_quickbooks_transfers
)
select
    *,
    md5("id" || '-' || "_airbyte_emitted_at") as _hash_id
from
    final

where
    _airbyte_emitted_at > (select max(_airbyte_emitted_at) from "datawarehouse".dev_quickbooks."transfers")
