
with __dbt__cte__clinics_meta_groups as (
select
    _airbyte_emitted_at,
    _airbyte_ab_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'id') as int) as id,
    cast(jsonb_extract_path_text(_airbyte_data, 'meta_group_name') as varchar(255)) as meta_group_name,
    cast(jsonb_extract_path_text(_airbyte_data, 'meta_template') as varchar(255)) as meta_template,
    cast(jsonb_extract_path_text(_airbyte_data, 'comments') as varchar(512)) as commentary,
    cast(jsonb_extract_path_text(_airbyte_data, 'created_at') as timestamp) as created_at,
    cast(jsonb_extract_path_text(_airbyte_data, 'updated_at') as timestamp) as updated_at,
    case cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_updated_at') as timestamp)
        when '1970-01-01'::timestamp then null
        else cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_updated_at') as timestamp)
    end
    as _ab_cdc_updated_at,
    cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_deleted_at') as timestamp) as _ab_cdc_deleted_at
from
    "datawarehouse".raw._airbyte_raw_goodpill_clinics_meta_groups
)select distinct on (meta_group_name) 
    id,
	meta_group_name as meta_group,
	meta_template,
	updated_at as date_processed
from __dbt__cte__clinics_meta_groups

	where updated_at > (select max(date_processed) from "datawarehouse".dev_analytics."clinics_groups")

order by meta_group_name, updated_at desc