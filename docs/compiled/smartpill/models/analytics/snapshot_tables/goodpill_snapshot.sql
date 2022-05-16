

-- Exclude the common columns between the tables,
--  to be called with dbt_utils.star (instead of using
--  the * operator to select all columns).



with  __dbt__cte__patients_max_events as (
select
	patient_id_cp,
	event_date as patient_event_date,
	event_name as patient_status,
	max(
      case
      when event_name = 'PATIENT_ACTIVE'
        then event_date
      else null
      end
    ) over(partition by patient_id_cp) as date_patient_active,
    max(
      case
      when event_name = 'PATIENT_NO_RX'
        then event_date
      else null
      end
    ) over(partition by patient_id_cp) as date_patient_no_rx, 
    max(
      case
      when event_name = 'PATIENT_UNREGISTERED'
        then event_date
      else null
      end
    ) over(partition by patient_id_cp) as date_patient_unregistered,
    max(
      case
      when event_name = 'PATIENT_DECEASED'
        then event_date
      else null
      end
    ) over(partition by patient_id_cp) as date_patient_deceased,
    max(
      case
      when event_name = 'PATIENT_CHURNED_NO_FILLABLE_RX'
        then event_date
      else null
      end
    ) over(partition by patient_id_cp) as date_patient_churned_no_fillable_rx,
    max(
      case
      when event_name = 'PATIENT_INACTIVE'
        then event_date
      else null
      end
    ) over(partition by patient_id_cp) as date_patient_inactive,
    max(
      case
      when event_name = 'PATIENT_CHURNED_OTHER'
        then event_date
      else null
      end
    ) over(partition by patient_id_cp) as date_patient_churned_other
      
from "datawarehouse".dev_analytics."patients_status_historic"
),  __dbt__cte__rxs_max_events as (
-- Exclude the common columns between the tables,
--  to be called with dbt_utils.star (instead of using
--  the * operator to select all columns).



select
	rh.patient_id_cp,
	event_date as rx_event_date,
	rh.rx_number,
	first_value(provider_npi) over (
		partition by rx_number
		order by case when provider_npi is not null then 0 else 1 end, event_date desc nulls last
	) as rx_provider_npi,
	first_value(drug_generic) over (
		partition by rx_number
		order by case when drug_generic is not null then 0 else 1 end, event_date desc nulls last
	) as rx_drug_generic,
    max(  
      case
      when rh.event_name = 'RX_UPDATED'
        then rh.event_date
      else null
      end
    ) over(partition by rx_number) as rx_date_updated,
  
    max(
      case
      when rh.event_name = 'RX_WRITTEN'
        then rh.event_date
      else null
      end
    ) over(partition by rx_number) as rx_date_written,
  
    max(
      case
      when rh.event_name = 'RX_TRANSFERRED'
        then rh.event_date
      else null
      end
    ) over(partition by rx_number) as date_rx_transferred,
  
    max(
      case
      when rh.event_name = 'RX_ADDED'
        then rh.event_date
      else null
      end
    ) over(partition by rx_number) as rx_date_added ,
	rh."clinic_name" as "rx_clinic_name",
	rh."is_refill" as "rx_is_refill",
	rh."rx_autofill" as "rx_autofill",
	rh."sig_qty_per_day" as "rx_sig_qty_per_day",
	rh."rx_message_key" as "rx_message_key",
	rh."max_gsn" as "rx_max_gsn",
	rh."drug_gsns" as "rx_drug_gsns",
	rh."refills_total" as "rx_refills_total",
	rh."refills_original" as "rx_refills_original",
	rh."refills_left" as "rx_refills_left",
	rh."refill_date_first" as "rx_refill_date_first",
	rh."refill_date_last" as "rx_refill_date_last",
	rh."rx_date_expired" as "rx_date_expired",
	rh."rx_date_changed" as "rx_date_changed",
	rh."qty_left" as "rx_qty_left",
	rh."qty_original" as "rx_qty_original",
	rh."sig_actual" as "rx_sig_actual",
	rh."sig_initial" as "rx_sig_initial",
	rh."sig_clean" as "rx_sig_clean",
	rh."sig_qty" as "rx_sig_qty",
	rh."sig_days" as "rx_sig_days",
	rh."sig_qty_per_day_actual" as "rx_sig_qty_per_day_actual",
	rh."sig_v2_qty" as "rx_sig_v2_qty",
	rh."sig_v2_days" as "rx_sig_v2_days",
	rh."sig_v2_qty_per_day" as "rx_sig_v2_qty_per_day",
	rh."sig_v2_unit" as "rx_sig_v2_unit",
	rh."sig_v2_conf_score" as "rx_sig_v2_conf_score",
	rh."sig_v2_dosages" as "rx_sig_v2_dosages",
	rh."sig_v2_scores" as "rx_sig_v2_scores",
	rh."sig_v2_frequencies" as "rx_sig_v2_frequencies",
	rh."sig_v2_durations" as "rx_sig_v2_durations",
	rh."refill_date_next" as "rx_refill_date_next",
	rh."refill_date_manual" as "rx_refill_date_manual",
	rh."refill_date_default" as "rx_refill_date_default",
	rh."qty_total" as "rx_qty_total",
	rh."rx_source" as "rx_source",
	rh."rx_transfer" as "rx_transfer"

from "datawarehouse".dev_analytics."rxs_historic" rh
),  __dbt__cte__clinics_providers_max_events as (
-- Exclude the common columns between the tables,
--  to be called with dbt_utils.star (instead of using
--  the * operator to select all columns).



select
    max(
      case
      when cph.event_name = 'CLINIC_META_ADDED'
        then cph.event_date
      else null
      end
    ) over(partition by (coupon_code, clinic_regular_name, npi_number, provider_name)) as date_clinic_meta_added,
    max(
      case
      when cph.event_name = 'CLINIC_META_UPDATED'
        then cph.event_date
      else null
      end
    ) over(partition by (coupon_code, clinic_regular_name, npi_number, provider_name)) as date_clinic_meta_updated,
	max(
      case
      when cph.event_name = 'CLINIC_META_DELETED'
        then cph.event_date
      else null
      end
    ) over(partition by (coupon_code, clinic_regular_name, npi_number, provider_name)) as date_clinic_meta_deleted,
	cph."coupon_code" as "clinic_coupon_code",
	cph."clinic_regular_name" as "clinic_regular_name",
	cph."npi_number" as "clinic_npi_number",
	cph."provider_name" as "clinic_provider_name",
	cph."clinic_name" as "clinic_name",
	cph."updated_at" as "clinic_updated_at",
	cph."event_date" as "clinic_event_date"
from "datawarehouse".dev_analytics."clinics_providers_historic" cph
),psh as (
	-- join with dimension patient right away, to join later with clinics
	select distinct on (patient_id_cp)
		pme.*,
		p.payment_coupon as patient_payment_coupon,
		p.tracking_coupon as patient_tracking_coupon
	from __dbt__cte__patients_max_events pme
	left join "datawarehouse".dev_analytics."patients" p using (patient_id_cp)
	order by patient_id_cp, patient_event_date desc
),

rh as (
	select
		rh.*,
		-- join with dimension provider right away, to join later with clinics
		p.provider_first_name,
		p.provider_last_name
	from __dbt__cte__rxs_max_events rh
	left join "datawarehouse".dev_analytics."providers" p on rh.rx_provider_npi = p.provider_npi
),

oi as (
	select
		patient_id_cp,
		rx_number,
		invoice_number,
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
		"item_added_by" as "date_order_item_added",
		"item_date_added" as "item_date_added",
		"refill_date_last" as "item_refill_date_last",
		"refill_date_manual" as "item_refill_date_manual",
		"refill_date_default" as "item_refill_date_default",
		"refill_target_date" as "item_refill_target_date",
		"refill_target_days" as "item_refill_target_days",
		"refill_target_rxs" as "item_refill_target_rxs"
	from "datawarehouse".dev_analytics."order_items" oi
),

oh as (
  select
	  patient_id_cp,
	  invoice_number,
    order_date_added as date_order_added,
    order_date_dispensed as date_order_dispensed,
    order_date_shipped as date_order_shipped,
    order_date_returned as date_order_returned,
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
	from "datawarehouse".dev_analytics."orders"
),

cph as (
	select
		*
	from __dbt__cte__clinics_providers_max_events
)

select distinct on (patient_id_cp, rx_number, invoice_number)
	*,
	coalesce(clinic_name, rh.rx_clinic_name) as clinic_coalesced_name
from psh
left join rh using (patient_id_cp)
left join oi using (rx_number, patient_id_cp)
left join oh using (invoice_number, patient_id_cp)
left join cph on
	(
		cph.clinic_coupon_code = coalesce(psh.patient_payment_coupon, psh.patient_tracking_coupon)
		or cph.clinic_regular_name = rh.rx_clinic_name
		or cph.clinic_npi_number = rh.rx_provider_npi
		or cph.clinic_provider_name = concat(trim(rh.provider_first_name), ' ', trim(rh.provider_last_name))
	) and (
		cph.date_clinic_meta_added <= rh.rx_event_date
		and (cph.date_clinic_meta_deleted is null or cph.date_clinic_meta_deleted > rh.rx_event_date)
	)
where
	coalesce(rh.rx_event_date, now()) <= coalesce(oh.date_order_dispensed)
order by
	patient_id_cp,
	rx_number,
	invoice_number,
	patient_event_date desc,
	rx_event_date desc,
	-- prioritize table for clinic name instead of timestamps or last event
	clinic_coupon_code desc nulls last,
	clinic_npi_number desc nulls last,
	clinic_regular_name desc nulls last,
	clinic_provider_name desc nulls last