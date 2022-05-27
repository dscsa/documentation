
with final as (
    select
        jsonb_extract_path_text(_airbyte_data, 'Id') as "id",
    STR_TO_DATE(jsonb_extract_path_text(_airbyte_data, 'MetaData','CreateTime'), '%Y-%m-%dT%H:%i:%s.%fZ') as "created_at",
    STR_TO_DATE(jsonb_extract_path_text(_airbyte_data, 'MetaData','LastUpdatedTime'), '%Y-%m-%dT%H:%i:%s.%fZ') as "updated_at",
    _airbyte_emitted_at,
        jsonb_extract_path_text(_airbyte_data, 'FullyQualifiedName') as fully_qualified_name,
        cast(jsonb_extract_path_text(_airbyte_data, 'Active') as 
    signed
 ) as is_active,
        cast(jsonb_extract_path_text(_airbyte_data, 'Balance') as 
    numeric(28, 6)
) as balance,
        cast(jsonb_extract_path_text(_airbyte_data, 'BalanceWithJobs') as 
    numeric(28, 6)
) as balance_with_jobs,
        cast(jsonb_extract_path_text(_airbyte_data, 'BillWithParent') as 
    signed
 ) as bill_with_parent,
        jsonb_extract_path_text(_airbyte_data, 'CompanyName') as company_name,
        jsonb_extract_path_text(_airbyte_data, 'CurrencyRef','name') as currency_name,
        jsonb_extract_path_text(_airbyte_data, 'DisplayName') as display_name,
        jsonb_extract_path_text(_airbyte_data, 'WebAddr','URI') as website,
        cast(jsonb_extract_path_text(_airbyte_data, 'Taxable') as 
    numeric(28, 6)
) as taxable
    from
        "datawarehouse"."raw"._airbyte_raw_quickbook_customers
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
