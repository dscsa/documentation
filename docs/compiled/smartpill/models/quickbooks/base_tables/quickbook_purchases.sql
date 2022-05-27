
with final as (
    select
        jsonb_extract_path_text(_airbyte_data, 'Id') as "id",
    STR_TO_DATE(jsonb_extract_path_text(_airbyte_data, 'MetaData','CreateTime'), '%Y-%m-%dT%H:%i:%s.%fZ') as "created_at",
    STR_TO_DATE(jsonb_extract_path_text(_airbyte_data, 'MetaData','LastUpdatedTime'), '%Y-%m-%dT%H:%i:%s.%fZ') as "updated_at",
    _airbyte_emitted_at,
        jsonb_extract_path_text(_airbyte_data, 'AccountRef','value') as account_id,
        jsonb_extract_path_text(_airbyte_data, 'DocNumber') as doc_number,
        jsonb_extract_path_text(_airbyte_data, 'Credit') as credit,
        jsonb_extract_path_text(_airbyte_data, 'CurrencyRef','name') as currency_name,
        cast(jsonb_extract_path_text(_airbyte_data, 'TotalAmt') as 
    numeric(28, 6)
) as total_amount,
        jsonb_extract_path_text(_airbyte_data, 'PaymentType') as payment_type,
        STR_TO_DATE(jsonb_extract_path_text(_airbyte_data, 'TxnDate'), '%Y-%m-%dT%H:%i:%s.%fZ') as transaction_date,
        jsonb_extract_path_text(_airbyte_data, 'EntityRef','value') as entity_id,
        jsonb_extract_path_text(_airbyte_data, 'EntityRef','type') as entity_type,
        jsonb_extract_path(_airbyte_data, 'Line') as line
    from
        "datawarehouse"."raw"._airbyte_raw_quickbook_purchases
)
select
    *,
    if(entity_type = 'Customer', entity_id, null) as customer_id,
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
