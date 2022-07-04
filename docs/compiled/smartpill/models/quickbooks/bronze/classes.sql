with final as (
    select
        jsonb_extract_path_text(_airbyte_data, 'Id') as "id",
        cast(jsonb_extract_path_text(_airbyte_data, 'MetaData','CreateTime') as timestamp) as "created_at",
        cast(jsonb_extract_path_text(_airbyte_data, 'MetaData','LastUpdatedTime') as timestamp) as "updated_at",
        _airbyte_emitted_at,
        jsonb_extract_path_text(_airbyte_data, 'FullyQualifiedName') as fully_qualified_name,
        cast(jsonb_extract_path_text(_airbyte_data, 'Active') as bool) as is_active,
        jsonb_extract_path_text(_airbyte_data, 'Name') as name,
        cast(jsonb_extract_path_text(_airbyte_data, 'SubClass') as bool) as is_subclass
    from
        "datawarehouse"."raw"._airbyte_raw_quickbooks_classes
)
select
    *,
    md5("id" || '-' || "_airbyte_emitted_at") as _hash_id
from
    final

where
    _airbyte_emitted_at > (select max(_airbyte_emitted_at) from "datawarehouse".dev_quickbooks."classes")
