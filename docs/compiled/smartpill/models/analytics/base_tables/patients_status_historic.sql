
with  __dbt__cte__order_items_max_events as (
-- Exclude the common columns between the tables,
--  to be called with dbt_utils.star (instead of using
--  the * operator to select all columns).


select
	patient_id_cp,
	event_date as item_event_date,
	rx_number,
	invoice_number,
	
  
    max(
      
      case
      when event_name = 'ORDER_ITEM_ADDED'
        then event_date
      else null
      end
    )
	
      over(partition by invoice_number, rx_number)
	
    
      
        as date_order_item_added
      
    
    ,
  
    max(
      
      case
      when event_name = 'ORDER_ITEM_UPDATED'
        then event_date
      else null
      end
    )
	
      over(partition by invoice_number, rx_number)
	
    
      
        as date_order_item_updated
      
    
    ,
  
    max(
      
      case
      when event_name = 'ORDER_ITEM_DELETED'
        then event_date
      else null
      end
    )
	
      over(partition by invoice_number, rx_number)
	
    
      
        as date_order_item_deleted
      
    
    
  
,
	"groups" as "item_groups",
  "rx_dispensed_id" as "item_rx_dispensed_id",
  "stock_level_initial" as "item_stock_level_initial",
  "rx_message_keys_initial" as "item_rx_message_keys_initial",
  "patient_autofill_initial" as "item_patient_autofill_initial",
  "rx_autofill_initial" as "item_rx_autofill_initial",
  "rx_numbers_initial" as "item_rx_numbers_initial",
  "zscore_initial" as "item_zscore_initial",
  "refills_dispensed_default" as "item_refills_dispensed_default",
  "refills_dispensed_actual" as "item_refills_dispensed_actual",
  "days_dispensed_default" as "item_days_dispensed_default",
  "days_dispensed_actual" as "item_days_dispensed_actual",
  "qty_dispensed_default" as "item_qty_dispensed_default",
  "qty_dispensed_actual" as "item_qty_dispensed_actual",
  "price_dispensed_default" as "item_price_dispensed_default",
  "price_dispensed_actual" as "item_price_dispensed_actual",
  "qty_pended_total" as "item_qty_pended_total",
  "qty_pended_repacks" as "item_qty_pended_repacks",
  "count_pended_total" as "item_count_pended_total",
  "count_pended_repacks" as "item_count_pended_repacks",
  "item_message_keys" as "item_message_keys",
  "item_message_text" as "item_message_text",
  "item_type" as "item_type",
  "item_added_by" as "item_added_by",
  "item_date_added" as "item_date_added",
  "refill_date_last" as "item_refill_date_last",
  "refill_date_manual" as "item_refill_date_manual",
  "refill_date_default" as "item_refill_date_default",
  "refill_target_date" as "item_refill_target_date",
  "refill_target_days" as "item_refill_target_days",
  "refill_target_rxs" as "item_refill_target_rxs"
from "datawarehouse".dev_analytics."order_items_historic" oih
),  __dbt__cte__orders_max_events as (
-- Exclude the common columns between the tables,
--  to be called with dbt_utils.star (instead of using
--  the * operator to select all columns).


select
	patient_id_cp,
	event_date as order_event_date,
	invoice_number,

    max(
      case
      when event_name = 'ORDER_ADDED'
        then event_date
      else null
      end
    ) over(partition by invoice_number) as date_order_added,
    max(
      case
      when event_name = 'ORDER_DISPENSED'
        then event_date
      else null
      end
    ) over(partition by invoice_number) as date_order_dispensed,
    max(
	case
      when event_name = 'ORDER_SHIPPED'
        then event_date
      else null
      end
    ) over(partition by invoice_number) as date_order_shipped,
  
    max(
      case
      when event_name = 'ORDER_DELETED'
        then event_date
      else null
      end
    ) over(partition by invoice_number) as date_order_deleted,
    max(
      case
      when event_name = 'ORDER_RETURNED'
        then event_date
      else null
      end
    ) over(partition by invoice_number) as date_order_returned ,

	"count_items" as "order_count_items",
	"count_filled" as "order_count_filled",
	"count_nofill" as "order_count_nofill",
	"order_source" as "order_source",
	"order_stage_cp" as "order_stage_cp",
	"order_status" as "order_status",
	"invoice_doc_id" as "order_invoice_doc_id",
	"tracking_number" as "order_tracking_number",
	"payment_total_default" as "order_payment_total_default",
	"payment_total_actual" as "order_payment_total_actual",
	"payment_fee_default" as "order_payment_fee_default",
	"payment_fee_actual" as "order_payment_fee_actual",
	"payment_due_default" as "order_payment_due_default",
	"payment_due_actual" as "order_payment_due_actual",
	"payment_date_autopay" as "order_payment_date_autopay",
	"payment_method_actual" as "order_payment_method_actual",
	"order_payment_coupon" as "order_payment_coupon",
	"order_note" as "order_note",
	"rph_check" as "order_rph_check",
	"tech_fill" as "order_tech_fill",
	"location_id" as "order_location_id"

from "datawarehouse".dev_analytics."orders_historic" oh
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
    "datawarehouse".dev_analytics."raw_gp_patients"
),calc_statuses as (
	with all_dates as (
		select distinct
			patient_id_cp,
			has_refills,
			rx_date_expired,
			date_order_added,
			date_order_shipped,
			refill_date_next,
			refill_date_first,
			coalesce(lag(date_order_added, -1) over (partition by patient_id_cp order by date_order_added), now()) as next_row_order_date_added
		from (
			select distinct on (rh.patient_id_cp, rh.rx_number, rh.refill_date_next, rh.event_date, oh.date_order_added)
				rh.patient_id_cp,
				oh.date_order_added as date_order_added,
				oh.date_order_shipped as date_order_shipped,
				rh.refill_date_next as refill_date_next,
				rh.refills_left > 0 or rh.refills_total > 0 as has_refills,
				rh.rx_date_expired,
				rh.refill_date_first
			from "datawarehouse".dev_analytics."rxs_historic" rh
			left join __dbt__cte__order_items_max_events oih using (rx_number)
			left join __dbt__cte__orders_max_events oh using (invoice_number)
			order by rh.patient_id_cp, rh.rx_number, rh.refill_date_next, rh.event_date, oh.date_order_added
		) t
	)
	select
		patient_id_cp,
		coalesce(date_order_added + interval '1' day, refill_date_first) as event_date,
		'PATIENT_ACTIVE' as event_name,
		null::varchar as _airbyte_ab_id,
		date_order_added as _airbyte_emitted_at,
		date_order_added as _ab_cdc_updated_at
	from all_dates
	where date_order_added is not null or has_refills
	union
	select
		patient_id_cp,
		coalesce(refill_date_next, date_order_added + interval '4' month) as event_date,
		'PATIENT_CHURNED_OTHER' as event_name,
		null::varchar as _airbyte_ab_id,
		date_order_added as _airbyte_emitted_at,
		date_order_added as _ab_cdc_updated_at
	from all_dates
	where
		coalesce(refill_date_next, date_order_added + interval '4' month) < next_row_order_date_added
		and date_order_shipped < next_row_order_date_added
		and has_refills and rx_date_expired <= coalesce(refill_date_next, date_order_added + interval '4' month)
	union
	select
		patient_id_cp,
		coalesce(refill_date_next, date_order_added + interval '4' month) as event_date,
		'PATIENT_CHURNED_NO_FILLABLE_RX' as event_name,
		null::varchar as _airbyte_ab_id,
		date_order_added as _airbyte_emitted_at,
		date_order_added as _ab_cdc_updated_at
	from all_dates
	where
		coalesce(refill_date_next, date_order_added + interval '4' month) < next_row_order_date_added
		and date_order_shipped < next_row_order_date_added
		and not (has_refills or rx_date_expired <= coalesce(refill_date_next, date_order_added + interval '4' month))
),
statuses as (
	select
		gp.patient_id_cp,
		patient_date_added as event_date,
		'PATIENT_UNREGISTERED' as event_name,
		_airbyte_ab_id,
		_airbyte_emitted_at,
		_ab_cdc_updated_at
		from __dbt__cte__gp_patients gp
		inner join (
			select distinct patient_id_cp from "datawarehouse".dev_analytics."rxs_historic" rxs
		) t using (patient_id_cp)
		where patient_date_added is not null
			and (patient_date_registered is null or date(patient_date_registered) >= date(patient_date_added))
	union
	select
		patient_id_cp,
		patient_date_registered as event_date,
		'PATIENT_NO_RX' as event_name,
		_airbyte_ab_id,
		_airbyte_emitted_at,
		_ab_cdc_updated_at
		from __dbt__cte__gp_patients
		where patient_date_registered is not null
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

	where event_date > (select MAX(event_date) from "datawarehouse".dev_analytics."patients_status_historic")

order by patient_id_cp, event_name, event_date desc