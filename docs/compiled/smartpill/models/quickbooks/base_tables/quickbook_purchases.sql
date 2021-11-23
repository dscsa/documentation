
with final as (
    select
        json_value(_airbyte_data, 
    '$."Id"') as `id`,
    STR_TO_DATE(json_value(_airbyte_data, 
    '$."MetaData"."CreateTime"'), '%Y-%m-%dT%H:%i:%s.%fZ') as `created_at`,
    STR_TO_DATE(json_value(_airbyte_data, 
    '$."MetaData"."LastUpdatedTime"'), '%Y-%m-%dT%H:%i:%s.%fZ') as `updated_at`,
    _airbyte_emitted_at,
        json_value(_airbyte_data, 
    '$."AccountRef"."value"') as account_id,
        json_value(_airbyte_data, 
    '$."DocNumber"') as doc_number,
        json_value(_airbyte_data, 
    '$."Credit"') as credit,
        json_value(_airbyte_data, 
    '$."CurrencyRef"."name"') as currency_name,
        cast(json_value(_airbyte_data, 
    '$."TotalAmt"') as 
    float
) as total_amount,
        json_value(_airbyte_data, 
    '$."PaymentType"') as payment_type,
        STR_TO_DATE(json_value(_airbyte_data, 
    '$."TxnDate"'), '%Y-%m-%dT%H:%i:%s.%fZ') as transaction_date,
        json_value(_airbyte_data, 
    '$."EntityRef"."value"') as entity_id,
        json_value(_airbyte_data, 
    '$."EntityRef"."type"') as entity_type,
        json_extract(_airbyte_data, 
    '$."Line"') as line
    from
        analytics_v2._airbyte_raw_quickbook_purchases
)
select
    *,
    if(entity_type = 'Customer', entity_id, null) as customer_id,
    md5(cast(concat(coalesce(cast(`id` as char), ''), '-', coalesce(cast(`_airbyte_emitted_at` as char), '')) as char)) as _hash_id
from
    final
where
    
    _airbyte_emitted_at > (select max(_airbyte_emitted_at) from analytics.`quickbook_purchases`)
