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