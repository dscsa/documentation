
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
    '$."FullyQualifiedName"') fully_qualified_name,
        cast(json_value(_airbyte_data, 
    '$."Active"') as 
    signed
 ) is_active,
        json_value(_airbyte_data, 
    '$."Name"') name,
        json_value(_airbyte_data, 
    '$."AcctNum"') as account_number,
        cast(json_value(_airbyte_data, 
    '$."SubAccount"') as 
    signed
) as is_sub_account,
        json_value(_airbyte_data, 
    '$."ParentRef"."value"') as parent_account_id,
        json_value(_airbyte_data, 
    '$."AccountType"') account_type,
        json_value(_airbyte_data, 
    '$."AccountSubType"') account_sub_type,
        json_value(_airbyte_data, 
    '$."Classification"') classification,
        cast(json_value(_airbyte_data, 
    '$."CurrentBalance"') as 
    float
) balance,
        cast(json_value(_airbyte_data, 
    '$."CurrentBalanceWithSubAccounts"') as 
    float
) balance_with_sub_accounts,
        json_value(_airbyte_data, 
    '$."CurrencyRef"."name"') currency_name,
        json_value(_airbyte_data, 
    '$."Description"') description
    from
        analytics_v2._airbyte_raw_quickbook_accounts
)
select
    *,
    md5(cast(concat(coalesce(cast(`id` as char), ''), '-', coalesce(cast(`_airbyte_emitted_at` as char), '')) as char)) as _hash_id
from
    final
where
    
    _airbyte_emitted_at > (select max(_airbyte_emitted_at) from analytics.`quickbook_accounts`)
