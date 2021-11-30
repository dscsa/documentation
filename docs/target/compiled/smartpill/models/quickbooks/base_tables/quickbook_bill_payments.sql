
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
    '$."CheckPayment"."BankAccountRef"."value"') as check_bank_account_id,
        json_value(_airbyte_data, 
    '$."CheckPayment"."PrintStatus"') as check_print_status,
        json_value(_airbyte_data, 
    '$."CreditCardPayment"."CCAccountRef"."value"') as credit_card_account_id,
        json_value(_airbyte_data, 
    '$."CurrencyRef"."name"') as currency_name,
        json_value(_airbyte_data, 
    '$."PayType"') as pay_type,
        cast(json_value(_airbyte_data, 
    '$."TotalAmt"') as 
    float
) as total_amount,
        STR_TO_DATE(json_value(_airbyte_data, 
    '$."TxnDate"'), '%Y-%m-%dT%H:%i:%s.%fZ') as transaction_date,
        json_extract(_airbyte_data, 
    '$."Line"') as line
    from
        analytics_v2._airbyte_raw_quickbook_bill_payments
)
select
    *,
    md5(cast(concat(coalesce(cast(`id` as char), ''), '-', coalesce(cast(`_airbyte_emitted_at` as char), '')) as char)) as _hash_id
from
    final
where
    
    _airbyte_emitted_at > (select max(_airbyte_emitted_at) from analytics.`quickbook_bill_payments`)
