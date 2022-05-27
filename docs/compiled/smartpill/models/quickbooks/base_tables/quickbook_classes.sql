
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
        jsonb_extract_path_text(_airbyte_data, 'Name') as name,
        cast(jsonb_extract_path_text(_airbyte_data, 'SubClass') as 
    signed
 ) as is_subclass
    from
        "datawarehouse"."raw"._airbyte_raw_quickbook_classes
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
