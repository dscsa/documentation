
with  __dbt__cte__gp_patients as (
select
    _airbyte_emitted_at,
    _airbyte_ab_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_id_cp') as int) as patient_id_cp,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_id_wc') as int) as patient_id_wc,
    cast(jsonb_extract_path_text(_airbyte_data, 'first_name') as varchar(80)) as first_name,
    cast(jsonb_extract_path_text(_airbyte_data, 'last_name') as varchar(80)) as last_name,
    cast(jsonb_extract_path_text(_airbyte_data, 'birth_date') as timestamp) as birth_date,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_note') as varchar(3072)) as patient_note,
    cast(jsonb_extract_path_text(_airbyte_data, 'phone1') as varchar(10)) as phone1,
    cast(jsonb_extract_path_text(_airbyte_data, 'phone2') as varchar(10)) as phone2,
    cast(jsonb_extract_path_text(_airbyte_data, 'email') as varchar(255)) as email,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_autofill') as int) as patient_autofill,
    cast(jsonb_extract_path_text(_airbyte_data, 'pharmacy_name') as varchar(50)) as pharmacy_name,
    cast(jsonb_extract_path_text(_airbyte_data, 'pharmacy_npi') as varchar(10)) as pharmacy_npi,
    cast(jsonb_extract_path_text(_airbyte_data, 'pharmacy_fax') as varchar(12)) as pharmacy_fax,
    cast(jsonb_extract_path_text(_airbyte_data, 'pharmacy_phone') as varchar(12)) as pharmacy_phone,
    cast(jsonb_extract_path_text(_airbyte_data, 'pharmacy_address') as varchar(255)) as pharmacy_address,
    cast(jsonb_extract_path_text(_airbyte_data, 'payment_card_type') as varchar(20)) as payment_card_type,
    cast(jsonb_extract_path_text(_airbyte_data, 'payment_card_last4') as varchar(4)) as payment_card_last4,
    cast(jsonb_extract_path_text(_airbyte_data, 'payment_card_date_expired') as timestamp) as payment_card_date_expired,
    cast(jsonb_extract_path_text(_airbyte_data, 'payment_method_default') as varchar(50)) as payment_method_default,
    cast(jsonb_extract_path_text(_airbyte_data, 'payment_coupon') as varchar(20)) as payment_coupon,
    cast(jsonb_extract_path_text(_airbyte_data, 'tracking_coupon') as varchar(20)) as tracking_coupon,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_address1') as varchar(255)) as patient_address1,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_address2') as varchar(255)) as patient_address2,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_city') as varchar(255)) as patient_city,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_state') as varchar(2)) as patient_state,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_zip') as varchar(5)) as patient_zip,
    cast(jsonb_extract_path_text(_airbyte_data, 'refills_used') as decimal(5,2)) as refills_used,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_status') as int) as patient_status,
    cast(jsonb_extract_path_text(_airbyte_data, 'language') as varchar) as language,
    cast(jsonb_extract_path_text(_airbyte_data, 'allergies_none') as varchar(80)) as allergies_none,
    cast(jsonb_extract_path_text(_airbyte_data, 'allergies_cephalosporins') as varchar(80)) as allergies_cephalosporins,
    cast(jsonb_extract_path_text(_airbyte_data, 'allergies_sulfa') as varchar(80)) as allergies_sulfa,
    cast(jsonb_extract_path_text(_airbyte_data, 'allergies_aspirin') as varchar(80)) as allergies_aspirin,
    cast(jsonb_extract_path_text(_airbyte_data, 'allergies_penicillin') as varchar(80)) as allergies_penicillin,
    cast(jsonb_extract_path_text(_airbyte_data, 'allergies_erythromycin') as varchar(80)) as allergies_erythromycin,
    cast(jsonb_extract_path_text(_airbyte_data, 'allergies_codeine') as varchar(80)) as allergies_codeine,
    cast(jsonb_extract_path_text(_airbyte_data, 'allergies_nsaids') as varchar(80)) as allergies_nsaids,
    cast(jsonb_extract_path_text(_airbyte_data, 'allergies_salicylates') as varchar(80)) as allergies_salicylates,
    cast(jsonb_extract_path_text(_airbyte_data, 'allergies_azithromycin') as varchar(80)) as allergies_azithromycin,
    cast(jsonb_extract_path_text(_airbyte_data, 'allergies_amoxicillin') as varchar(80)) as allergies_amoxicillin,
    cast(jsonb_extract_path_text(_airbyte_data, 'allergies_tetracycline') as varchar(80)) as allergies_tetracycline,
    cast(jsonb_extract_path_text(_airbyte_data, 'allergies_other') as varchar(255)) as allergies_other,
    cast(jsonb_extract_path_text(_airbyte_data, 'medications_other') as varchar(3072)) as medications_other,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_date_added') as timestamp) as patient_date_added,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_date_registered') as timestamp) as patient_date_registered,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_date_changed') as timestamp) as patient_date_changed,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_date_updated') as timestamp) as patient_date_updated,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_inactive') as varchar) as patient_inactive,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_date_first_dispensed') as timestamp) as patient_date_first_dispensed,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_date_first_rx_received') as timestamp) as patient_date_first_rx_received,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_date_first_expected_by') as timestamp) as patient_date_first_expected_by,
    cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_updated_at') as timestamp) as _ab_cdc_updated_at,
    cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_deleted_at') as timestamp) as _ab_cdc_deleted_at
from
    "datawarehouse".raw._airbyte_raw_goodpill_gp_patients
),calc_statuses as (
	with all_dates as (
		select distinct
			patient_id_cp,
			order_date_added,
			refill_date_next,
			coalesce(lag(order_date_added, -1) over (partition by patient_id_cp order by order_date_added), now()) as next_row_order_date_added
		from (
			select distinct on (oh.patient_id_cp, oih.invoice_number, oih.rx_number)
				oh.patient_id_cp,
				coalesce(oh.event_date, rh.refill_date_next) as order_date_added,
				coalesce(oh.event_date + interval '1 day' * oih.days_dispensed_actual, rh.refill_date_next) as refill_date_next
			from "datawarehouse".analytics."orders_historic" oh
			inner join "datawarehouse".analytics."order_items_historic" oih using (invoice_number)
			inner join "datawarehouse".analytics."rxs_historic" rh using (rx_number)
			where oh.event_name = 'ORDER_ADDED'
			order by oh.patient_id_cp, oih.invoice_number, oih.rx_number, coalesce(oh.event_date + interval '1 day' * oih.days_dispensed_actual, rh.refill_date_next) desc
		) t
	)
	select
		patient_id_cp,
		order_date_added as event_date,
		'PATIENT_ACTIVE' as event_name,
		null::varchar as _airbyte_ab_id,
		order_date_added _airbyte_emitted_at,
		order_date_added _ab_cdc_updated_at
	from all_dates
	union
	select
		patient_id_cp,
		patient_date_added + interval '7' day as event_date,
		'PATIENT_ACTIVE' as event_name,
		_airbyte_ab_id,
		_airbyte_emitted_at,
		_ab_cdc_updated_at
		from __dbt__cte__gp_patients
		where patient_date_added is not null
	union
	select
		patient_id_cp,
		refill_date_next + interval '30' day as event_date,
		'PATIENT_NEW_CHURN' as event_name,
		null::varchar as _airbyte_ab_id,
		refill_date_next + interval '30' day _airbyte_emitted_at,
		refill_date_next + interval '30' day _ab_cdc_updated_at
	from all_dates
	where refill_date_next + interval '30' day < next_row_order_date_added
	union
	select
		patient_id_cp,
		refill_date_next + interval '31' day as event_date,
		'PATIENT_CHURNED' as event_name,
		null::varchar as _airbyte_ab_id,
		refill_date_next + interval '31' day _airbyte_emitted_at,
		refill_date_next + interval '31' day _ab_cdc_updated_at
	from all_dates
	where refill_date_next + interval '31' day < next_row_order_date_added
),
statuses as (
	select
		patient_id_cp,
		patient_date_added as event_date,
		'PATIENT_NEW' as event_name,
		_airbyte_ab_id,
		_airbyte_emitted_at,
		_ab_cdc_updated_at
		from __dbt__cte__gp_patients
		where patient_date_added is not null
	union
	select
		patient_id_cp,
		patient_date_updated as event_date,
		'PATIENT_INACTIVE' as event_name,
		_airbyte_ab_id,
		_airbyte_emitted_at,
		_ab_cdc_updated_at
		from __dbt__cte__gp_patients
		where patient_inactive = 'Inactive'
	union
	select
		patient_id_cp,
		patient_date_updated as event_date,
		'PATIENT_DECEASED' as event_name,
		_airbyte_ab_id,
		_airbyte_emitted_at,
		_ab_cdc_updated_at
		from __dbt__cte__gp_patients
		where patient_inactive = 'Deceased'
)

/* select distinct on (patient_id_cp, event_name) */
select
	patient_id_cp,
	event_name,
	event_date,
	_airbyte_ab_id,
	_airbyte_emitted_at,
	_ab_cdc_updated_at,
	'GOODPILL' as _airbyte_source,
	md5(cast(concat(event_name, patient_id_cp, event_date) as 
    varchar
)) as unique_event_id
from (
	select *
	from calc_statuses
	union
	select *
	from statuses
) gps

	where _airbyte_emitted_at > (select MAX(_airbyte_emitted_at) from "datawarehouse".analytics."patients_status_historic")

order by patient_id_cp, event_name, _airbyte_emitted_at desc