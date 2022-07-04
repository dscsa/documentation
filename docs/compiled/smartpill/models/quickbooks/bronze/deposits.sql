with final as (
    select
        jsonb_extract_path_text(_airbyte_data, 'Id') as "id",
        cast(jsonb_extract_path_text(_airbyte_data, 'MetaData','CreateTime') as timestamp) as "created_at",
        cast(jsonb_extract_path_text(_airbyte_data, 'MetaData','LastUpdatedTime') as timestamp) as "updated_at",
        _airbyte_emitted_at,
        jsonb_extract_path_text(_airbyte_data, 'CurrencyRef','name') as currency_name,
        jsonb_extract_path_text(_airbyte_data, 'DepositToAccountRef','value') as account_id,
        cast(jsonb_extract_path_text(_airbyte_data, 'TotalAmt') as decimal) as total_amount,
        cast(jsonb_extract_path_text(_airbyte_data, 'TxnDate') as timestamp) as transaction_date,
        jsonb_extract_path(_airbyte_data, 'Line') as line
    from
        "datawarehouse"."raw"._airbyte_raw_quickbooks_deposits
)
select
    *,
    md5("id" || '-' || "_airbyte_emitted_at") as _hash_id
from
    final

where
    _airbyte_emitted_at > (select max(_airbyte_emitted_at) from "datawarehouse".dev_quickbooks."deposits")
