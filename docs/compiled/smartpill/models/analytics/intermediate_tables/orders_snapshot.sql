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