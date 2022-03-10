

-- Exclude the common columns between the tables,
--  to be called with dbt_utils.star (instead of using
--  the * operator to select all columns).



with psh as (
	select distinct on (patient_id_cp)
		patient_id_cp,
		event_date as patient_event_date,
		event_name as patient_status,
		
  
    max(
      
      case
      when event_name = 'PATIENT_ACTIVE'
        then event_date
      else null
      end
    )
	
      over(partition by patient_id_cp)
	
    
      
        as date_patient_active
      
    
    ,
  
    max(
      
      case
      when event_name = 'PATIENT_NO_RX'
        then event_date
      else null
      end
    )
	
      over(partition by patient_id_cp)
	
    
      
        as date_patient_no_rx
      
    
    ,
  
    max(
      
      case
      when event_name = 'PATIENT_UNREGISTERED'
        then event_date
      else null
      end
    )
	
      over(partition by patient_id_cp)
	
    
      
        as date_patient_unregistered
      
    
    ,
  
    max(
      
      case
      when event_name = 'PATIENT_CHURNED_NO_FILLABLE_RX'
        then event_date
      else null
      end
    )
	
      over(partition by patient_id_cp)
	
    
      
        as date_patient_churned_no_fillable_rx
      
    
    ,
  
    max(
      
      case
      when event_name = 'PATIENT_DECEASED'
        then event_date
      else null
      end
    )
	
      over(partition by patient_id_cp)
	
    
      
        as date_patient_deceased
      
    
    ,
  
    max(
      
      case
      when event_name = 'PATIENT_INACTIVE'
        then event_date
      else null
      end
    )
	
      over(partition by patient_id_cp)
	
    
      
        as date_patient_inactive
      
    
    ,
  
    max(
      
      case
      when event_name = 'PATIENT_CHURNED_OTHER'
        then event_date
      else null
      end
    )
	
      over(partition by patient_id_cp)
	
    
      
        as date_patient_churned_other
      
    
    
  

	from "datawarehouse".dev_analytics."patients_status_historic"
	order by patient_id_cp, patient_event_date desc
	/* inner join "datawarehouse".dev_analytics."goodpill_events" using (event_name) */
),

rh as (
	select
		rh.patient_id_cp,
		event_date as rx_event_date,
		rh.rx_number,
		
  
    max(
      
      case
      when rh.event_name = 'RX_UPDATED'
        then rh.event_date
      else null
      end
    )
	
      over(partition by rx_number)
	
    
      
        as date_rx_updated
      
    
    ,
  
    max(
      
      case
      when rh.event_name = 'RX_WRITTEN'
        then rh.event_date
      else null
      end
    )
	
      over(partition by rx_number)
	
    
      
        as date_rx_written
      
    
    ,
  
    max(
      
      case
      when rh.event_name = 'RX_TRANSFERRED'
        then rh.event_date
      else null
      end
    )
	
      over(partition by rx_number)
	
    
      
        as date_rx_transferred
      
    
    
  
,
		rh."drug_generic" as "rx_drug_generic",
  rh."clinic_name" as "rx_clinic_name",
  rh."provider_npi" as "rx_provider_npi",
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
	/* inner join "datawarehouse".dev_analytics."goodpill_events" using (event_name) */
	/* where rh.event_date <= coalesce(oh.event_date, NOW()) */
	/* 	-- triple check this... */
	/* 	and oh.event_name = 'ORDER_SHIPPED' or oh.event_name = 'ORDER_RETURNED' */
),

oih as (
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
      when event_name = 'ORDER_ITEM_DELETED'
        then event_date
      else null
      end
    )
	
      over(partition by invoice_number, rx_number)
	
    
      
        as date_order_item_deleted
      
    
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
	/* inner join "datawarehouse".dev_analytics."goodpill_events" using (event_name) */
),

oh as (
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
    )
	
      over(partition by invoice_number)
	
    
      
        as date_order_added
      
    
    ,
  
    max(
      
      case
      when event_name = 'ORDER_DISPENSED'
        then event_date
      else null
      end
    )
	
      over(partition by invoice_number)
	
    
      
        as date_order_dispensed
      
    
    ,
  
    max(
      
      case
      when event_name = 'ORDER_SHIPPED'
        then event_date
      else null
      end
    )
	
      over(partition by invoice_number)
	
    
      
        as date_order_shipped
      
    
    ,
  
    max(
      
      case
      when event_name = 'ORDER_DELETED'
        then event_date
      else null
      end
    )
	
      over(partition by invoice_number)
	
    
      
        as date_order_deleted
      
    
    ,
  
    max(
      
      case
      when event_name = 'ORDER_RETURNED'
        then event_date
      else null
      end
    )
	
      over(partition by invoice_number)
	
    
      
        as date_order_returned
      
    
    
  
,
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
	/* inner join "datawarehouse".dev_analytics."goodpill_events" using (event_name) */
)

select distinct on (patient_id_cp, rx_number, invoice_number)
	*
from psh
left join rh using (patient_id_cp)
left join oih using (rx_number, patient_id_cp)
left join oh using (invoice_number, patient_id_cp)
where
	coalesce(rh.rx_event_date, NOW()) <= coalesce(oh.order_event_date, NOW())
	and date_order_item_deleted is null
	and date_order_deleted is null
order by
	patient_id_cp,
	rx_number,
	invoice_number,
	rx_event_date desc,
	order_event_date desc
	/* coalesce(order_event_date, item_event_date, rx_event_date, patient_event_date) desc */