
with final as (
    select
        jsonb_extract_path_text(_airbyte_data, 'Id') as "id",
    STR_TO_DATE(jsonb_extract_path_text(_airbyte_data, 'MetaData','CreateTime'), '%Y-%m-%dT%H:%i:%s.%fZ') as "created_at",
    STR_TO_DATE(jsonb_extract_path_text(_airbyte_data, 'MetaData','LastUpdatedTime'), '%Y-%m-%dT%H:%i:%s.%fZ') as "updated_at",
    _airbyte_emitted_at,
        jsonb_extract_path_text(_airbyte_data, 'FullyQualifiedName') fully_qualified_name,
        cast(jsonb_extract_path_text(_airbyte_data, 'Active') as 
    signed
 ) is_active,
        jsonb_extract_path_text(_airbyte_data, 'Name') name,
        jsonb_extract_path_text(_airbyte_data, 'AcctNum') as account_number,
        cast(jsonb_extract_path_text(_airbyte_data, 'SubAccount') as 
    signed
) as is_sub_account,
        jsonb_extract_path_text(_airbyte_data, 'ParentRef','value') as parent_account_id,
        jsonb_extract_path_text(_airbyte_data, 'AccountType') account_type,
        jsonb_extract_path_text(_airbyte_data, 'AccountSubType') account_sub_type,
        jsonb_extract_path_text(_airbyte_data, 'Classification') classification,
        cast(jsonb_extract_path_text(_airbyte_data, 'CurrentBalance') as 
    numeric(28, 6)
) balance,
        cast(jsonb_extract_path_text(_airbyte_data, 'CurrentBalanceWithSubAccounts') as 
    numeric(28, 6)
) balance_with_sub_accounts,
        jsonb_extract_path_text(_airbyte_data, 'CurrencyRef','name') currency_name,
        jsonb_extract_path_text(_airbyte_data, 'Description') description
    from
        "datawarehouse".raw._airbyte_raw_quickbook_accounts
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
