with final as (
    select
        jsonb_extract_path_text(_airbyte_data, 'Id') as "id",
        cast(jsonb_extract_path_text(_airbyte_data, 'MetaData','CreateTime') as timestamp) as "created_at",
        cast(jsonb_extract_path_text(_airbyte_data, 'MetaData','LastUpdatedTime') as timestamp) as "updated_at",
        _airbyte_emitted_at,
        jsonb_extract_path_text(_airbyte_data, 'FullyQualifiedName') as fully_qualified_name,
        cast(jsonb_extract_path_text(_airbyte_data, 'Active') as bool) as is_active,
        cast(jsonb_extract_path_text(_airbyte_data, 'Balance') as decimal) as balance,
        cast(jsonb_extract_path_text(_airbyte_data, 'BalanceWithJobs') as decimal) as balance_with_jobs,
        cast(jsonb_extract_path_text(_airbyte_data, 'BillWithParent') as bool) bill_with_parent,
        jsonb_extract_path_text(_airbyte_data, 'CompanyName') as company_name,
        jsonb_extract_path_text(_airbyte_data, 'CurrencyRef','name') as currency_name,
        jsonb_extract_path_text(_airbyte_data, 'DisplayName') as display_name,
        jsonb_extract_path_text(_airbyte_data, 'WebAddr','URI') as website,
        cast(jsonb_extract_path_text(_airbyte_data, 'Taxable') as bool) as taxable
    from
        "datawarehouse"."raw"._airbyte_raw_quickbooks_customers
)
select
    *,
    md5("id" || '-' || "_airbyte_emitted_at") as _hash_id
from
    final

where
    _airbyte_emitted_at > (select max(_airbyte_emitted_at) from "datawarehouse".prod_quickbooks."customers")
