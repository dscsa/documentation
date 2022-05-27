
with final as (
    select
        jsonb_extract_path_text(_airbyte_data, 'Id') as "id",
    STR_TO_DATE(jsonb_extract_path_text(_airbyte_data, 'MetaData','CreateTime'), '%Y-%m-%dT%H:%i:%s.%fZ') as "created_at",
    STR_TO_DATE(jsonb_extract_path_text(_airbyte_data, 'MetaData','LastUpdatedTime'), '%Y-%m-%dT%H:%i:%s.%fZ') as "updated_at",
    _airbyte_emitted_at,
        jsonb_extract_path_text(_airbyte_data, 'DepositToAccountRef','value') as account_id,
        jsonb_extract_path_text(_airbyte_data, 'CustomerRef','value') as customer_id,
        cast(jsonb_extract_path_text(_airbyte_data, 'Balance') as 
    numeric(28, 6)
) as balace,
        jsonb_extract_path_text(_airbyte_data, 'DocNumber') as doc_number,
        jsonb_extract_path_text(_airbyte_data, 'CurrencyRef','name') as currency_name,
        cast(jsonb_extract_path_text(_airbyte_data, 'TotalAmt') as 
    numeric(28, 6)
) as total_amount,
        STR_TO_DATE(jsonb_extract_path_text(_airbyte_data, 'TxnDate'), '%Y-%m-%dT%H:%i:%s.%fZ') as transaction_date,
        STR_TO_DATE(jsonb_extract_path_text(_airbyte_data, 'DueDate'), '%Y-%m-%dT%H:%i:%s.%fZ') as due_date,
        jsonb_extract_path(_airbyte_data, 'Line') as line
    from
        "datawarehouse"."raw"._airbyte_raw_quickbook_invoices
)
select
    *,
    md5(cast(coalesce(cast("id" as 
    varchar
), '') || '-' || coalesce(cast("_airbyte_emitted_at" as 
    varchar
), '') as 
    varchar
)) as _hash_id
from
    final
where
    
    true
