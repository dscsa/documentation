with  __dbt__cte__clinics_meta_coupons as (
select
    _airbyte_emitted_at,
    _airbyte_ab_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'id') as int) as id,
    cast(jsonb_extract_path_text(_airbyte_data, 'coupon_code') as varchar(255)) as coupon_code,
    cast(jsonb_extract_path_text(_airbyte_data, 'clinic_name') as varchar(255)) as clinic_name,
    cast(jsonb_extract_path_text(_airbyte_data, 'comments') as varchar(512)) as commentary,
    cast(jsonb_extract_path_text(_airbyte_data, 'created_at') as timestamp) as created_at,
    cast(jsonb_extract_path_text(_airbyte_data, 'updated_at') as timestamp) as updated_at,
    case cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_updated_at') as timestamp)
        when '1970-01-01'::timestamp then null
        else cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_updated_at') as timestamp)
    end
    as _ab_cdc_updated_at,
    cast(
        jsonb_extract_path_text(_airbyte_data, '_ab_cdc_deleted_at') as timestamp
    ) as _ab_cdc_deleted_at
from
    "datawarehouse".raw._airbyte_raw_goodpill_clinics_meta_coupons
),  __dbt__cte__clinics_meta_normalized_names as (
select
    _airbyte_emitted_at,
    _airbyte_ab_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'id') as int) as id,
    cast(jsonb_extract_path_text(_airbyte_data, 'clinic_name') as varchar(255)) as clinic_name,
    cast(
        jsonb_extract_path_text(_airbyte_data, 'clinic_normalized_name') as varchar(255)
    ) as clinic_normalized_name,
    cast(jsonb_extract_path_text(_airbyte_data, 'comments') as varchar(255)) as commentary,
    cast(jsonb_extract_path_text(_airbyte_data, 'created_at') as timestamp) as created_at,
    cast(jsonb_extract_path_text(_airbyte_data, 'updated_at') as timestamp) as updated_at,
    case cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_updated_at') as timestamp)
        when '1970-01-01'::timestamp then null
        else cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_updated_at') as timestamp)
    end
    as _ab_cdc_updated_at,
    cast(
        jsonb_extract_path_text(_airbyte_data, '_ab_cdc_deleted_at') as timestamp
    ) as _ab_cdc_deleted_at
from
    "datawarehouse".raw._airbyte_raw_goodpill_clinics_meta_normalized_names
),  __dbt__cte__clinics_meta_npi_numbers as (
select
    _airbyte_emitted_at,
    _airbyte_ab_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'id') as int) as id,
    cast(jsonb_extract_path_text(_airbyte_data, 'npi_number') as varchar) as npi_number,
    cast(jsonb_extract_path_text(_airbyte_data, 'provider_name') as varchar(255)) as provider_name,
    cast(jsonb_extract_path_text(_airbyte_data, 'clinic_name') as varchar(255)) as clinic_name,
    cast(jsonb_extract_path_text(_airbyte_data, 'comments') as varchar(255)) as commentary,
    cast(jsonb_extract_path_text(_airbyte_data, 'created_at') as timestamp) as created_at,
    cast(jsonb_extract_path_text(_airbyte_data, 'updated_at') as timestamp) as updated_at,
    case cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_updated_at') as timestamp)
        when '1970-01-01'::timestamp then null
        else cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_updated_at') as timestamp)
    end
    as _ab_cdc_updated_at,
    cast(
        jsonb_extract_path_text(_airbyte_data, '_ab_cdc_deleted_at') as timestamp
    ) as _ab_cdc_deleted_at
from
    "datawarehouse".raw._airbyte_raw_goodpill_clinics_meta_npi_numbers
),  __dbt__cte__clinics_meta_provider_names as (
select
    _airbyte_emitted_at,
    _airbyte_ab_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'id') as int) as id,
    cast(jsonb_extract_path_text(_airbyte_data, 'provider_name') as varchar(255)) as provider_name,
    cast(jsonb_extract_path_text(_airbyte_data, 'clinic_name') as varchar(255)) as clinic_name,
    cast(jsonb_extract_path_text(_airbyte_data, 'comments') as varchar(255)) as commentary,
    cast(jsonb_extract_path_text(_airbyte_data, 'created_at') as timestamp) as created_at,
    cast(jsonb_extract_path_text(_airbyte_data, 'updated_at') as timestamp) as updated_at,
    case cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_updated_at') as timestamp)
        when '1970-01-01'::timestamp then null
        else cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_updated_at') as timestamp)
    end
    as _ab_cdc_updated_at,
    cast(
        jsonb_extract_path_text(_airbyte_data, '_ab_cdc_deleted_at') as timestamp
    ) as _ab_cdc_deleted_at
from
    "datawarehouse".raw._airbyte_raw_goodpill_clinics_meta_provider_names
),clinics_meta as (
	select
		coupon_code,
		null::varchar as clinic_regular_name,
		null::varchar as npi_number,
		null::varchar as provider_name,
		clinic_name,
		created_at,
		updated_at,
		_ab_cdc_deleted_at,
		_airbyte_ab_id,
		'clinics_meta_coupons' as _dbt_source
	from
		__dbt__cte__clinics_meta_coupons
	union
	select
		null::varchar as coupon_code,
		clinic_name as clinic_regular_name,
		null::varchar as npi_number,
		null::varchar as provider_name,
		clinic_normalized_name as clinic_name,
		created_at,
		updated_at,
		_ab_cdc_deleted_at,
		_airbyte_ab_id,
		'clinics_meta_normalized_names' as _dbt_source
	from
		__dbt__cte__clinics_meta_normalized_names
	union
	select
		null::varchar as coupon_code,
		null::varchar as clinic_regular_name,
		npi_number,
		provider_name,
		clinic_name,
		created_at,
		updated_at,
		_ab_cdc_deleted_at,
		_airbyte_ab_id,
		'clinics_meta_npi_numbers' as _dbt_source
	from
		__dbt__cte__clinics_meta_npi_numbers
	union
	select
		null::varchar as coupon_code,
		null::varchar as clinic_regular_name,
		null::varchar as npi_number,
		provider_name,
		clinic_name,
		created_at,
		updated_at,
		_ab_cdc_deleted_at,
		_airbyte_ab_id,
		'clinics_meta_provider_names' as _dbt_source
	from
		__dbt__cte__clinics_meta_provider_names
),

events as (
	select
		*,
		'CLINIC_META_ADDED' as event_name,
		created_at as event_date
	from clinics_meta
	union
	select
		*,
		'CLINIC_META_UPDATED' as event_name,
		updated_at as event_date
	from clinics_meta
	where created_at <> updated_at
	union
	select
		*,
		'CLINIC_META_DELETED' as event_name,
		_ab_cdc_deleted_at as event_date
	from clinics_meta
	where _ab_cdc_deleted_at is not null
)

select
	*
from events

	where event_date > (select MAX(event_date) from "datawarehouse".dev_analytics."clinics_providers_historic")
