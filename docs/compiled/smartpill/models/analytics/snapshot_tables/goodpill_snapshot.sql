

-- Exclude the common columns between the tables,
--  to be called with dbt_utils.star (instead of using
--  the * operator to select all columns).



with psh as (
	select distinct on (patient_id_cp)
		patient_id_cp,
		
  
    max(
      
      case
      when event_name = 'PATIENT_ADDED'
        then event_date
      else null
      end
    )
	
      over(partition by patient_id_cp)
	
    
      
        as date_patient_added
      
    
    ,
  
    max(
      
      case
      when event_name = 'PATIENT_REGISTERED'
        then event_date
      else null
      end
    )
	
      over(partition by patient_id_cp)
	
    
      
        as date_patient_registered
      
    
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
      when event_name = 'PATIENT_DECEASED'
        then event_date
      else null
      end
    )
	
      over(partition by patient_id_cp)
	
    
      
        as date_patient_deceased
      
    
    
  

	from "datawarehouse".analytics."patients_status_historic"
	inner join "datawarehouse".analytics."goodpill_events" using (event_name)
	order by patient_id_cp, event_weight desc, event_date desc
),

rh as (
	select distinct on (rx_number)
		patient_id_cp,
		rx_number,
		"drug_generic" as "rx_drug_generic",
  "clinic_name" as "rx_clinic_name",
  "provider_npi" as "rx_provider_npi",
  "is_refill" as "rx_is_refill",
  "rx_autofill" as "rx_autofill",
  "sig_qty_per_day" as "rx_sig_qty_per_day",
  "rx_message_key" as "rx_message_key",
  "max_gsn" as "rx_max_gsn",
  "drug_gsns" as "rx_drug_gsns",
  "refills_total" as "rx_refills_total",
  "refills_original" as "rx_refills_original",
  "refills_left" as "rx_refills_left",
  "refill_date_first" as "rx_refill_date_first",
  "refill_date_last" as "rx_refill_date_last",
  "rx_date_expired" as "rx_date_expired",
  "rx_date_changed" as "rx_date_changed",
  "qty_left" as "rx_qty_left",
  "qty_original" as "rx_qty_original",
  "sig_actual" as "rx_sig_actual",
  "sig_initial" as "rx_sig_initial",
  "sig_clean" as "rx_sig_clean",
  "sig_qty" as "rx_sig_qty",
  "sig_days" as "rx_sig_days",
  "sig_qty_per_day_actual" as "rx_sig_qty_per_day_actual",
  "sig_v2_qty" as "rx_sig_v2_qty",
  "sig_v2_days" as "rx_sig_v2_days",
  "sig_v2_qty_per_day" as "rx_sig_v2_qty_per_day",
  "sig_v2_unit" as "rx_sig_v2_unit",
  "sig_v2_conf_score" as "rx_sig_v2_conf_score",
  "sig_v2_dosages" as "rx_sig_v2_dosages",
  "sig_v2_scores" as "rx_sig_v2_scores",
  "sig_v2_frequencies" as "rx_sig_v2_frequencies",
  "sig_v2_durations" as "rx_sig_v2_durations",
  "refill_date_next" as "rx_refill_date_next",
  "refill_date_manual" as "rx_refill_date_manual",
  "refill_date_default" as "rx_refill_date_default",
  "qty_total" as "rx_qty_total",
  "rx_source" as "rx_source",
  "rx_transfer" as "rx_transfer",
		
  
    max(
      
      case
      when event_name = 'RX_WRITTEN'
        then event_date
      else null
      end
    )
	
      over(partition by rx_number)
	
    
      
        as date_rx_written
      
    
    ,
  
    max(
      
      case
      when event_name = 'RX_ADDED'
        then event_date
      else null
      end
    )
	
      over(partition by rx_number)
	
    
      
        as date_rx_added
      
    
    ,
  
    max(
      
      case
      when event_name = 'RX_UPDATED'
        then event_date
      else null
      end
    )
	
      over(partition by rx_number)
	
    
      
        as date_rx_updated
      
    
    ,
  
    max(
      
      case
      when event_name = 'RX_TRANSFERRED'
        then event_date
      else null
      end
    )
	
      over(partition by rx_number)
	
    
      
        as date_rx_transferred
      
    
    
  

	from "datawarehouse".analytics."rxs_historic" rh
	inner join "datawarehouse".analytics."goodpill_events" using (event_name)
	order by rx_number, event_weight desc, event_date desc
),

oih as (
	select distinct on (invoice_number, rx_number)
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
  "item_date_added" as "item_date_added",
  "refill_date_last" as "item_refill_date_last",
  "refill_date_manual" as "item_refill_date_manual",
  "refill_date_default" as "item_refill_date_default",
  "refill_target_date" as "item_refill_target_date",
  "refill_target_days" as "item_refill_target_days",
  "refill_target_rxs" as "item_refill_target_rxs",
		
  
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
      
    
    
  

	from "datawarehouse".analytics."order_items_historic" oih
	inner join "datawarehouse".analytics."goodpill_events" using (event_name)
	order by invoice_number, rx_number, event_weight desc, event_date desc
),

oh as (
	select distinct on (invoice_number)
		patient_id_cp,
		invoice_number,
		"location_id" as "order_location_id",
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
  "coupon_lines" as "order_coupon_lines",
  "order_note" as "order_note",
  "rph_check" as "order_rph_check",
  "tech_fill" as "order_tech_fill",
		
  
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
      
    
    
  

	from "datawarehouse".analytics."orders_historic" oh
	inner join "datawarehouse".analytics."goodpill_events" using (event_name)
	order by invoice_number, event_date desc, event_weight desc
)

select distinct on (patient_id_cp, rx_number, invoice_number)
	*
from psh
left join rh using (patient_id_cp)
left join oih using (rx_number, patient_id_cp)
left join oh using (invoice_number, patient_id_cp)