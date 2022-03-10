

-- depends on __dbt__cte__an_fills_logs
-- depends on __dbt__cte__an_patients
-- depends on __dbt__cte__an_providers
-- depends on __dbt__cte__an_drugs
-- depends on __dbt__cte__an_clinics
-- depends on __dbt__cte__gp_rxs_single
-- depends on __dbt__cte__gp_rxs_single
-- depends on __dbt__cte__gp_rxs_single
-- depends on __dbt__cte__gp_rxs_grouped
-- depends on __dbt__cte__gp_patients

with  __dbt__cte__an_fills_logs as (
select
	_airbyte_ab_id,
	_airbyte_emitted_at,
	cast(jsonb_extract_path_text(_airbyte_data, 'patient_id') as int) as patient_id,
	cast(jsonb_extract_path_text(_airbyte_data, 'event_date_id') as int) as event_date_id,
	cast(jsonb_extract_path_text(_airbyte_data, 'event_time_id') as int) as event_time_id,
	cast(jsonb_extract_path_text(_airbyte_data, 'event_type') as varchar(15)) as event_type,
	cast(jsonb_extract_path_text(_airbyte_data, 'goodpill_event_date') as timestamp) as goodpill_event_date,
	cast(jsonb_extract_path_text(_airbyte_data, 'rx_number') as int) as rx_number,
	cast(jsonb_extract_path_text(_airbyte_data, 'provider_id') as int) as provider_id,
	cast(jsonb_extract_path_text(_airbyte_data, 'drug_id') as int) as drug_id,
	cast(jsonb_extract_path_text(_airbyte_data, 'clinic_id') as int) as clinic_id,
	cast(jsonb_extract_path_text(_airbyte_data, 'location_id') as int) as location_id,
	cast(jsonb_extract_path_text(_airbyte_data, 'invoice_number') as int) as invoice_number,
	cast(jsonb_extract_path_text(_airbyte_data, 'is_refill') as int) as is_refill,
	cast(jsonb_extract_path_text(_airbyte_data, 'rx_autofill') as int) as rx_autofill,
	cast(jsonb_extract_path_text(_airbyte_data, 'sig_qty_per_day') as decimal(6,3)) as sig_qty_per_day,
	cast(jsonb_extract_path_text(_airbyte_data, 'rx_message_key') as varchar(80)) as rx_message_key,
	cast(jsonb_extract_path_text(_airbyte_data, 'max_gsn') as int) as max_gsn,
	cast(jsonb_extract_path_text(_airbyte_data, 'drug_gsns') as varchar(255)) as drug_gsns,
	cast(jsonb_extract_path_text(_airbyte_data, 'refills_total') as decimal(5,2)) as refills_total,
	cast(jsonb_extract_path_text(_airbyte_data, 'refills_original') as decimal(5,2)) as refills_original,
	cast(jsonb_extract_path_text(_airbyte_data, 'refills_left') as decimal(5,2)) as refills_left,
	cast(jsonb_extract_path_text(_airbyte_data, 'refill_date_first') as date) as refill_date_first,
	cast(jsonb_extract_path_text(_airbyte_data, 'refill_date_last') as date) as refill_date_last,
	cast(jsonb_extract_path_text(_airbyte_data, 'rx_date_changed') as timestamp) as rx_date_changed,
	cast(jsonb_extract_path_text(_airbyte_data, 'rx_date_expired') as date) as rx_date_expired,
	cast(jsonb_extract_path_text(_airbyte_data, 'qty_left') as decimal(10,3)) as qty_left,
	cast(jsonb_extract_path_text(_airbyte_data, 'qty_original') as decimal(10,3)) as qty_original,
	cast(jsonb_extract_path_text(_airbyte_data, 'stock_level_initial') as varchar(80)) as stock_level_initial,
	cast(jsonb_extract_path_text(_airbyte_data, 'sig_actual') as varchar(255)) as sig_actual,
	cast(jsonb_extract_path_text(_airbyte_data, 'sig_initial') as varchar(255)) as sig_initial,
	cast(jsonb_extract_path_text(_airbyte_data, 'sig_clean') as varchar(255)) as sig_clean,
	cast(jsonb_extract_path_text(_airbyte_data, 'sig_qty') as decimal(10,3)) as sig_qty,
	cast(jsonb_extract_path_text(_airbyte_data, 'sig_days') as int) as sig_days,
	cast(jsonb_extract_path_text(_airbyte_data, 'sig_qty_per_day_actual') as decimal(10,3)) as sig_qty_per_day_actual,
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
	cast(jsonb_extract_path_text(_airbyte_data, 'refill_date_next') as date) as refill_date_next,
	cast(jsonb_extract_path_text(_airbyte_data, 'refill_date_manual') as date) as refill_date_manual,
	cast(jsonb_extract_path_text(_airbyte_data, 'refill_date_default') as date) as refill_date_default,
	cast(jsonb_extract_path_text(_airbyte_data, 'qty_total') as decimal(11,3)) as qty_total,
	cast(jsonb_extract_path_text(_airbyte_data, 'rx_source') as varchar(80)) as rx_source,
	cast(jsonb_extract_path_text(_airbyte_data, 'rx_transfer') as varchar(80)) as rx_transfer,
	cast(jsonb_extract_path_text(_airbyte_data, 'rx_message_keys_initial') as varchar(255)) as rx_message_keys_initial,
	cast(jsonb_extract_path_text(_airbyte_data, 'patient_autofill_initial') as int) as patient_autofill_initial,
	cast(jsonb_extract_path_text(_airbyte_data, 'rx_autofill_initial') as int) as rx_autofill_initial,
	cast(jsonb_extract_path_text(_airbyte_data, 'rx_numbers_initial') as varchar(255)) as rx_numbers_initial,
	cast(jsonb_extract_path_text(_airbyte_data, 'zscore_initial') as decimal(6,3)) as zscore_initial,
	cast(jsonb_extract_path_text(_airbyte_data, 'refills_dispensed_actual') as decimal(5,2)) as refills_dispensed_actual,
	cast(jsonb_extract_path_text(_airbyte_data, 'days_dispensed_actual') as int) as days_dispensed_actual,
	cast(jsonb_extract_path_text(_airbyte_data, 'days_dispensed_actual_rx30') as int) as days_dispensed_actual_rx30,
	cast(jsonb_extract_path_text(_airbyte_data, 'qty_dispensed_actual') as decimal(10,3)) as qty_dispensed_actual,
	cast(jsonb_extract_path_text(_airbyte_data, 'qty_dispensed_actual_rx30') as decimal(10,3)) as qty_dispensed_actual_rx30,
	cast(jsonb_extract_path_text(_airbyte_data, 'price_dispensed_actual') as decimal(5,2)) as price_dispensed_actual,
	cast(jsonb_extract_path_text(_airbyte_data, 'count_items') as int) as count_items,
	cast(jsonb_extract_path_text(_airbyte_data, 'count_filled') as int) as count_filled,
	cast(jsonb_extract_path_text(_airbyte_data, 'count_nofill') as int) as count_nofill,
	cast(jsonb_extract_path_text(_airbyte_data, 'total_value_coalesced') as decimal(11,3)) as total_value_coalesced,
	cast(jsonb_extract_path_text(_airbyte_data, 'unit_value_coalesced') as decimal(11,3)) as unit_value_coalesced,
	cast(jsonb_extract_path_text(_airbyte_data, 'drug_values') as text) as drug_values,
	cast(jsonb_extract_path_text(_airbyte_data, 'extra_data') as text) as extra_data,
	cast(jsonb_extract_path_text(_airbyte_data, 'date_processed') as timestamp) as date_processed
from "datawarehouse".raw._airbyte_raw_analytics_fills_logs
),  __dbt__cte__an_patients as (
select
	_airbyte_ab_id,
	_airbyte_emitted_at,
	cast(jsonb_extract_path_text(_airbyte_data, 'patient_id') as int) as patient_id,
	cast(jsonb_extract_path_text(_airbyte_data, 'goodpill_id') as int) as goodpill_id,
	cast(jsonb_extract_path_text(_airbyte_data, 'patient_date_registered') as timestamp) as patient_date_registered,
	cast(jsonb_extract_path_text(_airbyte_data, 'patient_date_added') as timestamp) as patient_date_added,
	cast(jsonb_extract_path_text(_airbyte_data, 'fill_next') as date) as fill_next,
	cast(jsonb_extract_path_text(_airbyte_data, 'days_overdue') as int) as days_overdue,
	cast(jsonb_extract_path_text(_airbyte_data, 'first_name') as varchar(255)) as first_name,
	cast(jsonb_extract_path_text(_airbyte_data, 'last_name') as varchar(255)) as last_name,
	cast(jsonb_extract_path_text(_airbyte_data, 'birth_date') as timestamp) as birth_date,
	cast(jsonb_extract_path_text(_airbyte_data, 'email') as varchar(255)) as email,
	cast(jsonb_extract_path_text(_airbyte_data, 'phone_number') as varchar(255)) as phone_number,
	cast(jsonb_extract_path_text(_airbyte_data, 'phone_number_2') as varchar(255)) as phone_number_2,
	cast(jsonb_extract_path_text(_airbyte_data, 'address') as varchar(255)) as address,
	cast(jsonb_extract_path_text(_airbyte_data, 'address_2') as varchar(255)) as address_2,
	cast(jsonb_extract_path_text(_airbyte_data, 'city') as varchar(255)) as city,
	cast(jsonb_extract_path_text(_airbyte_data, 'state') as varchar(255)) as state,
	cast(jsonb_extract_path_text(_airbyte_data, 'zip_code') as varchar(255)) as zip_code,
	cast(jsonb_extract_path_text(_airbyte_data, 'payment_card_type') as varchar(20)) as payment_card_type,
	cast(jsonb_extract_path_text(_airbyte_data, 'payment_card_last4') as varchar(4)) as payment_card_last4,
	cast(jsonb_extract_path_text(_airbyte_data, 'payment_card_date_expired') as date) as payment_card_date_expired,
	cast(jsonb_extract_path_text(_airbyte_data, 'payment_method_default') as varchar(50)) as payment_method_default,
	cast(jsonb_extract_path_text(_airbyte_data, 'payment_coupon') as varchar(20)) as payment_coupon,
	cast(jsonb_extract_path_text(_airbyte_data, 'tracking_coupon') as varchar(20)) as tracking_coupon,
	cast(jsonb_extract_path_text(_airbyte_data, 'refills_used') as decimal(5,2)) as refills_used,
	cast(jsonb_extract_path_text(_airbyte_data, 'date_processed') as timestamp) as date_processed
from "datawarehouse".raw._airbyte_raw_analytics_patients
),  __dbt__cte__an_providers as (
select
	_airbyte_ab_id,
	_airbyte_emitted_at,
	cast(jsonb_extract_path_text(_airbyte_data, 'provider_id') as int) as provider_id,
	cast(jsonb_extract_path_text(_airbyte_data, 'npi') as varchar(255)) as npi,
	cast(jsonb_extract_path_text(_airbyte_data, 'first_name') as varchar(255)) as first_name,
	cast(jsonb_extract_path_text(_airbyte_data, 'last_name') as varchar(255)) as last_name,
	cast(jsonb_extract_path_text(_airbyte_data, 'phone_number') as varchar(255)) as phone_number,
	cast(jsonb_extract_path_text(_airbyte_data, 'phone_number_2') as varchar(255)) as phone_number_2,
	cast(jsonb_extract_path_text(_airbyte_data, 'address') as varchar(255)) as address,
	cast(jsonb_extract_path_text(_airbyte_data, 'city') as varchar(255)) as city,
	cast(jsonb_extract_path_text(_airbyte_data, 'state') as varchar(255)) as state,
	cast(jsonb_extract_path_text(_airbyte_data, 'zip_code') as varchar(255)) as zip_code,
	cast(jsonb_extract_path_text(_airbyte_data, 'date_processed') as timestamp) as date_processed
from "datawarehouse".raw._airbyte_raw_analytics_providers
),  __dbt__cte__an_drugs as (
select
	_airbyte_ab_id,
	_airbyte_emitted_at,
	cast(jsonb_extract_path_text(_airbyte_data, 'drug_id') as int) as drug_id,
	cast(jsonb_extract_path_text(_airbyte_data, 'generic_name') as varchar(255)) as generic_name,
	cast(jsonb_extract_path_text(_airbyte_data, 'brand_name') as varchar(255)) as brand_name,
	cast(jsonb_extract_path_text(_airbyte_data, 'gsns') as varchar(255)) as gsns,
	cast(jsonb_extract_path_text(_airbyte_data, 'current_price_per_month') as decimal(10,4)) as current_price_per_month,
	cast(jsonb_extract_path_text(_airbyte_data, 'current_price_30') as decimal(10,4)) as current_price_30,
	cast(jsonb_extract_path_text(_airbyte_data, 'current_price_90') as decimal(10,4)) as current_price_90,
	cast(jsonb_extract_path_text(_airbyte_data, 'current_value_awp') as decimal(10,4)) as current_value_awp,
	cast(jsonb_extract_path_text(_airbyte_data, 'current_value_goodrx') as decimal(10,4)) as current_value_goodrx,
	cast(jsonb_extract_path_text(_airbyte_data, 'current_value_nadac') as decimal(10,4)) as current_value_nadac,
	cast(jsonb_extract_path_text(_airbyte_data, 'current_value_coalesced') as decimal(10,4)) as current_value_coalesced,
	cast(jsonb_extract_path_text(_airbyte_data, 'date_processed') as timestamp) as date_processed
from "datawarehouse".raw._airbyte_raw_analytics_drugs
),  __dbt__cte__an_clinics as (
select
	_airbyte_ab_id,
	_airbyte_emitted_at,
	cast(jsonb_extract_path_text(_airbyte_data, 'clinic_id') as int) as clinic_id,
	cast(jsonb_extract_path_text(_airbyte_data, 'name_clean') as varchar(255)) as name_clean,
	cast(jsonb_extract_path_text(_airbyte_data, 'name_meta') as varchar(255)) as name_meta,
	cast(jsonb_extract_path_text(_airbyte_data, 'name') as varchar(255)) as name,
	cast(jsonb_extract_path_text(_airbyte_data, 'phone_number') as varchar(255)) as phone_number,
	cast(jsonb_extract_path_text(_airbyte_data, 'phone_number_2') as varchar(255)) as phone_number_2,
	cast(jsonb_extract_path_text(_airbyte_data, 'address') as varchar(255)) as address,
	cast(jsonb_extract_path_text(_airbyte_data, 'city') as varchar(255)) as city,
	cast(jsonb_extract_path_text(_airbyte_data, 'state') as varchar(255)) as state,
	cast(jsonb_extract_path_text(_airbyte_data, 'zip_code') as varchar(255)) as zip_code,
	cast(jsonb_extract_path_text(_airbyte_data, 'date_processed') as timestamp) as date_processed
from "datawarehouse".raw._airbyte_raw_analytics_clinics
),  __dbt__cte__gp_rxs_single as (
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
	cast(jsonb_extract_path_text(_airbyte_data, 'rx_date_added') as timestamp) as rx_date_added,
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
),rxh as (

	with grse as (
		(select distinct on (rx_number)
			*,
			'RX_ADDED' as event_name,
			rx_date_added as event_date
			from __dbt__cte__gp_rxs_single gprxs
			where (select sum(cast(event_name = 'RX_ADDED' as int)) from "datawarehouse".dev_analytics."rxs_historic" rhs where gprxs.rx_number = rhs.rx_number) = 0
			order by rx_number, _airbyte_emitted_at, _ab_cdc_updated_at)
		union
		(select distinct on (rx_number)
			*,
			'RX_TRANSFERRED' as event_name,
			rx_date_transferred as event_date
			from __dbt__cte__gp_rxs_single gprxs
			where rx_date_transferred is not null
				and (select sum(cast(event_name = 'RX_TRANSFERRED' as int)) from "datawarehouse".dev_analytics."rxs_historic" rhs where gprxs.rx_number = rhs.rx_number) = 0
			order by rx_number, _airbyte_emitted_at, _ab_cdc_updated_at)
		union
		(select distinct on (rx_number)
			*,
			-- rx_date_expired - 1 YEAR will be the written date, since when the rxs is inserted
			-- this value is populated with the current timestamp + 1 YEAR
			'RX_WRITTEN' as event_name,
			rx_date_expired - INTERVAL '1 year' as event_date
			from __dbt__cte__gp_rxs_single gprxs
			where
				rx_gsn is not null
				and rx_gsn <> 0
				and rx_date_expired is not null
				and (select sum(cast(event_name = 'RX_WRITTEN' as int)) from "datawarehouse".dev_analytics."rxs_historic" rhs where gprxs.rx_number = rhs.rx_number) = 0
			order by rx_number, _airbyte_emitted_at, _ab_cdc_updated_at)
		union
		select
			*,
			'RX_UPDATED' as event_name,
			_ab_cdc_updated_at as event_date
			from __dbt__cte__gp_rxs_single
			where _ab_cdc_updated_at is not null
		union
		select
			*,
			'RX_DELETED' as event_name,
			_ab_cdc_deleted_at as event_date
			from __dbt__cte__gp_rxs_single
			where _ab_cdc_deleted_at is not null
	), grg as (
		select distinct on (rx_numbers)
			rx_numbers,
			rx_autofill,
			refills_total,
			refill_date_first,
			refill_date_last,
			refill_date_next,
			refill_date_manual,
			refill_date_default,
			qty_total,
			sig_qty_per_day,
			max_gsn,
			drug_gsns
		from __dbt__cte__gp_rxs_grouped
		where _ab_cdc_deleted_at is null
		order by rx_numbers, _ab_cdc_updated_at desc
	), gp as (
		select distinct on (patient_id_cp)
			patient_id_cp,
			patient_autofill
		from __dbt__cte__gp_patients
		where _ab_cdc_deleted_at is null
		order by patient_id_cp, _ab_cdc_updated_at desc
	)
	select
		grse.rx_number,
		grse.patient_id_cp,
		grse.drug_generic,
		grse.provider_clinic as clinic_name,
		grse.provider_npi,
		case
			when grg.refill_date_first < grg.refill_date_last then 1
			else 0
		end as is_refill,
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
		null::timestamp as refill_date_next_max,
		grg.refill_date_manual,
		grg.refill_date_default,
		grg.qty_total,
		grse.rx_source,
		grse.rx_transfer,
		grse.event_name,
		grse.event_date,
		grse._airbyte_emitted_at,
		grse._ab_cdc_updated_at,
		'GOODPILL' as _airbyte_source,
		grse._airbyte_ab_id
	from grse
	inner join gp using (patient_id_cp)
	left join grg on grg.rx_numbers like CONCAT('%,', grse.rx_number ,',%')
	where grse._ab_cdc_updated_at > (select MAX(_airbyte_emitted_at) from "datawarehouse".dev_analytics."rxs_historic")
	order by rx_number, event_name, _airbyte_emitted_at desc


)
select
	rx_number,
	patient_id_cp,
	drug_generic,
	clinic_name,
	provider_npi,
	is_refill,
	rx_autofill,
	sig_qty_per_day,
	rx_message_key,
	max_gsn,
	drug_gsns,
	refills_total,
	refills_original,
	refills_left,
	refill_date_first,
	refill_date_last,
	rx_date_expired,
	rx_date_changed,
	qty_left,
	qty_original,
	sig_actual,
	sig_initial,
	sig_clean,
	sig_qty,
	sig_days,
	sig_qty_per_day_actual,
	sig_v2_qty,
	sig_v2_days,
	sig_v2_qty_per_day,
	sig_v2_unit,
	sig_v2_conf_score,
	sig_v2_dosages,
	sig_v2_scores,
	sig_v2_frequencies,
	sig_v2_durations,
	coalesce(refill_date_next_max, refill_date_next) as refill_date_next,
	refill_date_manual,
	refill_date_default,
	qty_total,
	rx_source,
	rx_transfer,
	event_name,
	event_date,
	_airbyte_ab_id,
	_airbyte_emitted_at as _airbyte_emitted_at,
	_ab_cdc_updated_at as _ab_cdc_updated_at,
	_airbyte_source,
	md5(cast(event_name || rx_number || event_date as 
    varchar
)) as unique_event_id
from rxh