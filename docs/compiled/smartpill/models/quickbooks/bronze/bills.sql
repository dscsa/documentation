with final as (
    select
        jsonb_extract_path_text(_airbyte_data, 'Id') as id,
        cast(jsonb_extract_path_text(_airbyte_data, 'MetaData','CreateTime') as timestamp) as created_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'MetaData','LastUpdatedTime') as timestamp) as updated_at,
        _airbyte_emitted_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'Balance') as decimal) as balance,
        jsonb_extract_path_text(_airbyte_data, 'CurrencyRef','name') as currency_name,
        cast(jsonb_extract_path_text(_airbyte_data, 'ExchangeRate') as decimal) as exchange_rate,
        cast(jsonb_extract_path_text(_airbyte_data, 'TotalAmt') as decimal) as total_amount,
        cast(jsonb_extract_path_text(_airbyte_data, 'DueDate') as timestamp) as due_date,
        cast(jsonb_extract_path_text(_airbyte_data, 'TxnDate') as timestamp) as transaction_date,
        jsonb_extract_path_text(_airbyte_data, 'APAccountRef','value') as payable_account_id,
        jsonb_extract_path_text(_airbyte_data, 'PrivateNote') as private_note,
        jsonb_extract_path(_airbyte_data, 'Line') as line
    from
        "datawarehouse"."raw"._airbyte_raw_quickbooks_bills
)
select
    *,
    md5("id" || '-' || "_airbyte_emitted_at") as _hash_id
from
    final

where
    _airbyte_emitted_at > (select max(_airbyte_emitted_at) from "datawarehouse".quickbooks."bills")
