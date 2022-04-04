

with r as (
	select
		_airbyte_emitted_at,
		_airbyte_ab_id,
		_airbyte_data,
		cast(jsonb_extract_path_text(_airbyte_data, 'patient_id_cp') as int) as pkey,
		cast(jsonb_extract_path_text(_airbyte_data, 'patient_date_added') as timestamp) as created_at,
		cast(jsonb_extract_path_text(_airbyte_data, 'patient_date_updated') as timestamp) as updated_at,
		cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_updated_at') as timestamp) as _ab_cdc_updated_at,
		cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_deleted_at') as timestamp) as _ab_cdc_deleted_at
	from
		"datawarehouse".raw._airbyte_raw_goodpill_gp_patients
)

select distinct on (pkey, created_at, updated_at, _ab_cdc_updated_at, _ab_cdc_deleted_at)
	*,
	md5(cast(concat(pkey, created_at, updated_at, _ab_cdc_updated_at, _ab_cdc_deleted_at) as 
    varchar
)) as unique_id
from
	r

	where greatest(created_at, updated_at, _ab_cdc_deleted_at) >= (select max(greatest(created_at, updated_at, date(_ab_cdc_deleted_at))) from "datawarehouse".dev_analytics."raw_gp_patients")	

order by
	pkey,
	created_at desc,
	updated_at desc,
	_ab_cdc_updated_at desc,
	_ab_cdc_deleted_at desc,
	-- It's rare, but there might be two rows with same timestamps, pk and different data
	-- To ensure the latest one, order by log pos
	cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_log_pos') as int) desc