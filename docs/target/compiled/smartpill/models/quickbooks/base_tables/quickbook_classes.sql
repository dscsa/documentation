
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
    '$."FullyQualifiedName"') as fully_qualified_name,
        cast(json_value(_airbyte_data, 
    '$."Active"') as 
    signed
 ) as is_active,
        json_value(_airbyte_data, 
    '$."Name"') as name,
        cast(json_value(_airbyte_data, 
    '$."SubClass"') as 
    signed
 ) as is_subclass
    from
        analytics_v2._airbyte_raw_quickbook_classes
)
select
    *,
    md5(cast(concat(coalesce(cast(`id` as char), ''), '-', coalesce(cast(`_airbyte_emitted_at` as char), '')) as char)) as _hash_id
from
    final
where
    
    _airbyte_emitted_at > (select max(_airbyte_emitted_at) from analytics.`quickbook_classes`)
