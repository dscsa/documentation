with raw_goodpill_clinics_meta_groups as (
    select
        cast(jsonb_extract_path_text(_airbyte_data, 'id') as int) as id,
        cast(jsonb_extract_path_text(_airbyte_data, 'meta_group_name') as varchar(255)) as meta_group,
        cast(jsonb_extract_path_text(_airbyte_data, 'meta_template') as varchar(255)) as meta_template,
        cast(jsonb_extract_path_text(_airbyte_data, 'comments') as varchar(512)) as commentary,
        cast(jsonb_extract_path_text(_airbyte_data, 'created_at') as timestamp) as created_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'updated_at') as timestamp) as updated_at
    from "datawarehouse"."raw"._airbyte_raw_goodpill_clinics_meta_groups
)

select distinct on (meta_group)
    id,
    nullif(meta_group, '') as meta_group,
    nullif(meta_template, '') as meta_template,
    nullif(commentary, '') as commentary,
    created_at,
    updated_at
from
    raw_goodpill_clinics_meta_groups

order by meta_group, updated_at desc