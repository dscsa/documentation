

with  __dbt__cte__gp_rxs_single as (
select
    _airbyte_emitted_at,
    _airbyte_ab_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'rx_number') as int) as rx_number,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_id_cp') as int) as patient_id_cp,
    cast(jsonb_extract_path_text(_airbyte_data, 'drug_generic') as varchar(255)) as drug_generic,
    cast(jsonb_extract_path_text(_airbyte_data, 'drug_brand') as varchar(255)) as drug_brand,
    cast(jsonb_extract_path_text(_airbyte_data, 'drug_name') as varchar(255)) as drug_name,
    cast(jsonb_extract_path_text(_airbyte_data, 'rx_message_key') as varchar(80)) as rx_message_key,
    cast(jsonb_extract_path_text(_airbyte_data, 'rx_message_text') as varchar(255)) as rx_message_text,
    cast(jsonb_extract_path_text(_airbyte_data, 'rx_gsn') as int) as rx_gsn,
    cast(jsonb_extract_path_text(_airbyte_data, 'drug_gsns') as varchar(255)) as drug_gsns,
    cast(jsonb_extract_path_text(_airbyte_data, 'refills_left') as decimal(5,2)) as refills_left,
    cast(jsonb_extract_path_text(_airbyte_data, 'refills_original') as decimal(5,2)) as refills_original,
    cast(jsonb_extract_path_text(_airbyte_data, 'qty_left') as decimal(10,3)) as qty_left,
    cast(jsonb_extract_path_text(_airbyte_data, 'qty_original') as decimal(10,3)) as qty_original,
    cast(jsonb_extract_path_text(_airbyte_data, 'sig_actual') as varchar(255)) as sig_actual,
    cast(jsonb_extract_path_text(_airbyte_data, 'sig_initial') as varchar(255)) as sig_initial,
    cast(jsonb_extract_path_text(_airbyte_data, 'sig_clean') as varchar(255)) as sig_clean,
    cast(jsonb_extract_path_text(_airbyte_data, 'sig_qty') as decimal(10,3)) as sig_qty,
    cast(jsonb_extract_path_text(_airbyte_data, 'sig_v1_qty') as decimal(10,3)) as sig_v1_qty,
    cast(jsonb_extract_path_text(_airbyte_data, 'sig_v1_days') as int) as sig_v1_days,
    cast(jsonb_extract_path_text(_airbyte_data, 'sig_v1_qty_per_day') as decimal(10,2)) as sig_v1_qty_per_day,
    cast(jsonb_extract_path_text(_airbyte_data, 'sig_days') as int) as sig_days,
    cast(jsonb_extract_path_text(_airbyte_data, 'sig_qty_per_day_default') as decimal(6,3)) as sig_qty_per_day_default,
    cast(jsonb_extract_path_text(_airbyte_data, 'sig_qty_per_day_actual') as decimal(6,3)) as sig_qty_per_day_actual,
    cast(jsonb_extract_path_text(_airbyte_data, 'sig_durations') as varchar(255)) as sig_durations,
    cast(jsonb_extract_path_text(_airbyte_data, 'sig_qtys_per_time') as varchar(255)) as sig_qtys_per_time,
    cast(jsonb_extract_path_text(_airbyte_data, 'sig_frequencies') as varchar(255)) as sig_frequencies,
    cast(jsonb_extract_path_text(_airbyte_data, 'sig_frequency_numerators') as varchar(255)) as sig_frequency_numerators,
    cast(jsonb_extract_path_text(_airbyte_data, 'sig_frequency_denominators') as varchar(255)) as sig_frequency_denominators,
    cast(jsonb_extract_path_text(_airbyte_data, 'sig_v2_qty') as decimal(10,3)) as sig_v2_qty,
    cast(jsonb_extract_path_text(_airbyte_data, 'sig_v2_days') as int) as sig_v2_days,
    cast(jsonb_extract_path_text(_airbyte_data, 'sig_v2_qty_per_day') as decimal(10,3)) as sig_v2_qty_per_day,
    cast(jsonb_extract_path_text(_airbyte_data, 'sig_v2_unit') as varchar(255)) as sig_v2_unit,
    cast(jsonb_extract_path_text(_airbyte_data, 'sig_v2_conf_score') as decimal(10,3)) as sig_v2_conf_score,
    cast(jsonb_extract_path_text(_airbyte_data, 'sig_v2_dosages') as varchar(255)) as sig_v2_dosages,
    cast(jsonb_extract_path_text(_airbyte_data, 'sig_v2_scores') as varchar(255)) as sig_v2_scores,
    cast(jsonb_extract_path_text(_airbyte_data, 'sig_v2_frequencies') as varchar(255)) as sig_v2_frequencies,
    cast(jsonb_extract_path_text(_airbyte_data, 'sig_v2_durations') as varchar(255)) as sig_v2_durations,
    cast(jsonb_extract_path_text(_airbyte_data, 'rx_autofill') as int) as rx_autofill,
    cast(jsonb_extract_path_text(_airbyte_data, 'refill_date_first') as timestamp) as refill_date_first,
    cast(jsonb_extract_path_text(_airbyte_data, 'refill_date_last') as timestamp) as refill_date_last,
    cast(jsonb_extract_path_text(_airbyte_data, 'refill_date_manual') as timestamp) as refill_date_manual,
    cast(jsonb_extract_path_text(_airbyte_data, 'refill_date_default') as timestamp) as refill_date_default,
    cast(jsonb_extract_path_text(_airbyte_data, 'rx_status') as int) as rx_status,
    cast(jsonb_extract_path_text(_airbyte_data, 'rx_stage') as varchar(80)) as rx_stage,
    cast(jsonb_extract_path_text(_airbyte_data, 'rx_source') as varchar(80)) as rx_source,
    cast(jsonb_extract_path_text(_airbyte_data, 'rx_transfer') as varchar(80)) as rx_transfer,
    cast(jsonb_extract_path_text(_airbyte_data, 'rx_date_transferred') as timestamp) as rx_date_transferred,
    cast(jsonb_extract_path_text(_airbyte_data, 'provider_npi') as varchar(80)) as provider_npi,
    cast(jsonb_extract_path_text(_airbyte_data, 'provider_first_name') as varchar(80)) as provider_first_name,
    cast(jsonb_extract_path_text(_airbyte_data, 'provider_last_name') as varchar(80)) as provider_last_name,
    cast(jsonb_extract_path_text(_airbyte_data, 'provider_clinic') as varchar(80)) as provider_clinic,
    cast(jsonb_extract_path_text(_airbyte_data, 'provider_phone') as varchar(10)) as provider_phone,
    cast(jsonb_extract_path_text(_airbyte_data, 'rx_date_changed') as timestamp) as rx_date_changed,
    cast(jsonb_extract_path_text(_airbyte_data, 'rx_date_expired') as timestamp) as rx_date_expired,
    cast(jsonb_extract_path_text(_airbyte_data, 'created_at') as timestamp) as created_at,
    cast(jsonb_extract_path_text(_airbyte_data, 'updated_at') as timestamp) as updated_at,
    cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_updated_at') as timestamp) as _ab_cdc_updated_at,
    cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_deleted_at') as timestamp) as _ab_cdc_deleted_at
from 
    "datawarehouse".raw._airbyte_raw_goodpill_gp_rxs_single
),  __dbt__cte__gp_rxs_grouped as (
select
    _airbyte_emitted_at,
    _airbyte_ab_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_id_cp') as int) as patient_id_cp,
    cast(jsonb_extract_path_text(_airbyte_data, 'drug_generic') as varchar(255)) as drug_generic,
    cast(jsonb_extract_path_text(_airbyte_data, 'drug_brand') as varchar(255)) as drug_brand,
    cast(jsonb_extract_path_text(_airbyte_data, 'drug_name') as varchar(255)) as drug_name,
    cast(jsonb_extract_path_text(_airbyte_data, 'sig_qty_per_day') as decimal(6,3)) as sig_qty_per_day,
    cast(jsonb_extract_path_text(_airbyte_data, 'rx_message_keys') as varchar(255)) as rx_message_keys,
    cast(jsonb_extract_path_text(_airbyte_data, 'max_gsn') as int) as max_gsn,
    cast(jsonb_extract_path_text(_airbyte_data, 'drug_gsns') as varchar(255)) as drug_gsns,
    cast(jsonb_extract_path_text(_airbyte_data, 'refills_total') as decimal(5,2)) as refills_total,
    cast(jsonb_extract_path_text(_airbyte_data, 'qty_total') as decimal(11,3)) as qty_total,
    cast(jsonb_extract_path_text(_airbyte_data, 'rx_autofill') as int) as rx_autofill,
    cast(jsonb_extract_path_text(_airbyte_data, 'refill_date_first') as timestamp) as refill_date_first,
    cast(jsonb_extract_path_text(_airbyte_data, 'refill_date_last') as timestamp) as refill_date_last,
    cast(jsonb_extract_path_text(_airbyte_data, 'refill_date_next') as timestamp) as refill_date_next,
    cast(jsonb_extract_path_text(_airbyte_data, 'refill_date_manual') as timestamp) as refill_date_manual,
    cast(jsonb_extract_path_text(_airbyte_data, 'refill_date_default') as timestamp) as refill_date_default,
    cast(jsonb_extract_path_text(_airbyte_data, 'best_rx_number') as int) as best_rx_number,
    cast(jsonb_extract_path_text(_airbyte_data, 'rx_numbers') as varchar(255)) as rx_numbers,
    cast(jsonb_extract_path_text(_airbyte_data, 'rx_sources') as varchar(80)) as rx_sources,
    cast(jsonb_extract_path_text(_airbyte_data, 'rx_date_changed') as timestamp) as rx_date_changed,
    cast(jsonb_extract_path_text(_airbyte_data, 'rx_date_expired') as timestamp) as rx_date_expired,
    cast(jsonb_extract_path_text(_airbyte_data, 'rx_date_transferred') as timestamp) as rx_date_transferred,
    cast(jsonb_extract_path_text(_airbyte_data, 'created_at') as timestamp) as created_at,
    cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_updated_at') as timestamp) as _ab_cdc_updated_at,
    cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_deleted_at') as timestamp) as _ab_cdc_deleted_at
from 
    "datawarehouse".raw._airbyte_raw_goodpill_gp_rxs_grouped
),  __dbt__cte__gp_patients as (
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
    cast(jsonb_extract_path_text(_airbyte_data, 'language') as int) as language,
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
    cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_updated_at') as timestamp) as _ab_cdc_updated_at,
    cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_deleted_at') as timestamp) as _ab_cdc_deleted_at
from
    "datawarehouse".raw._airbyte_raw_goodpill_gp_patients
),grse as (
	-- maybe this should be split in two cases (incremental vs not incremental)
	-- just like orders_historic
	select
		*,
		'RX_TRANSFERRED' as event_name,
		rx_date_transferred as event_date
		from __dbt__cte__gp_rxs_single
		where rx_date_transferred is not null
	union
	select
		*,
		-- rx_date_expired - 1 YEAR will be the written date, since when the rxs is inserted
		-- this value is populated with the current timestamp + 1 YEAR
		'RX_WRITTEN' as event_name,
		rx_date_expired - INTERVAL '1 year' as event_date
		from __dbt__cte__gp_rxs_single
		where rx_gsn is not null and rx_gsn <> 0 and rx_date_expired is not null
	union
	select
		*,
		'RX_DELETED' as event_name,
		_ab_cdc_deleted_at as event_date
		from __dbt__cte__gp_rxs_single
		where _ab_cdc_deleted_at is not null
), grg as (
	select distinct on (rx_numbers)
		*
	from __dbt__cte__gp_rxs_grouped
	where _ab_cdc_deleted_at is null
	order by rx_numbers, _ab_cdc_updated_at desc
), gp as (
	select distinct on (patient_id_cp)
		*
	from __dbt__cte__gp_patients
	where _ab_cdc_deleted_at is null
	order by patient_id_cp, _ab_cdc_updated_at desc
)

select distinct on (rx_number, event_name)
	grse.rx_number,
	grse.patient_id_cp,
	grse.drug_brand,
	grse.drug_generic,
	grse.provider_clinic,
	grse.provider_npi,
	case
		when grg.refill_date_first < grg.refill_date_last then 1
		else 0
	end as is_autofill,
	case
		when grg.rx_autofill = 1 and gp.patient_autofill = 1 then 1
		else 0
	end as rx_autofill,
	grg.sig_qty_per_day,
	grse.rx_message_key,
	grg.max_gsn,
	grg.drug_gsns,
	grg.refills_total,
	grse.refills_original,
	grse.refills_left,
	grse.refill_date_first,
	grse.refill_date_last,
	grse.rx_date_expired,
	grse.rx_date_changed,
	grse.qty_left,
	grse.qty_original,
	grse.sig_actual,
	grse.sig_initial,
	grse.sig_clean,
	grse.sig_qty,
	grse.sig_days,
	grse.sig_qty_per_day_actual,
	grse.sig_v2_qty,
	grse.sig_v2_days,
	grse.sig_v2_qty_per_day,
	grse.sig_v2_unit,
	grse.sig_v2_conf_score,
	grse.sig_v2_dosages,
	grse.sig_v2_scores,
	grse.sig_v2_frequencies,
	grse.sig_v2_durations,
	grg.refill_date_next,
	grg.refill_date_manual,
	grg.refill_date_default,
	grg.qty_total,
	grse.rx_source,
	grse.rx_transfer,
	grse.event_name,
	grse.event_date,
	greatest(grse._airbyte_emitted_at, grg._airbyte_emitted_at, gp._airbyte_emitted_at) as _airbyte_emitted_at,
	greatest(grse._ab_cdc_updated_at, grg._ab_cdc_updated_at, gp._ab_cdc_updated_at) as _ab_cdc_updated_at,
	md5(cast(event_name || rx_number as 
    varchar
)) as unique_event_id
from grse
inner join gp using (patient_id_cp)
left join grg on grg.rx_numbers like CONCAT('%,', grse.rx_number ,',%')

	where grse._ab_cdc_updated_at > (select MAX(_airbyte_emitted_at) from "datawarehouse".analytics."rxs_historic")

order by rx_number, event_name, _airbyte_emitted_at desc