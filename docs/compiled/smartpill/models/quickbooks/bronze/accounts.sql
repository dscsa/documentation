with final as (
    select
        jsonb_extract_path_text(_airbyte_data, 'Id') as id,
        cast(jsonb_extract_path_text(_airbyte_data, 'MetaData','CreateTime') as timestamp) as created_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'MetaData','LastUpdatedTime') as timestamp) as updated_at,
        _airbyte_emitted_at,
        jsonb_extract_path_text(_airbyte_data, 'FullyQualifiedName') fully_qualified_name,
        cast(jsonb_extract_path_text(_airbyte_data, 'Active') as bool) as is_active,
        jsonb_extract_path_text(_airbyte_data, 'Name') as name,
        jsonb_extract_path_text(_airbyte_data, 'AcctNum') as account_number,
        cast(jsonb_extract_path_text(_airbyte_data, 'SubAccount') as bool) as is_sub_account,
        jsonb_extract_path_text(_airbyte_data, 'ParentRef','value') as parent_account_id,
        jsonb_extract_path_text(_airbyte_data, 'AccountType') as account_type,
        jsonb_extract_path_text(_airbyte_data, 'AccountSubType') as account_sub_type,
        jsonb_extract_path_text(_airbyte_data, 'Classification') as classification,
        cast(jsonb_extract_path_text(_airbyte_data, 'CurrentBalance') as decimal) as balance,
        cast(jsonb_extract_path_text(_airbyte_data, 'CurrentBalanceWithSubAccounts') as decimal) as balance_with_sub_accounts,
        jsonb_extract_path_text(_airbyte_data, 'CurrencyRef','name') as currency_name,
        jsonb_extract_path_text(_airbyte_data, 'Description') as description
    from
        "datawarehouse"."raw"._airbyte_raw_quickbooks_accounts
)
select
    *,
    md5("id" || '-' || "_airbyte_emitted_at") as _hash_id
from
    final

where
    _airbyte_emitted_at > (select max(_airbyte_emitted_at) from "datawarehouse".quickbooks."accounts")
