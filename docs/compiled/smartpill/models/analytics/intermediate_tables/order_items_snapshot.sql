-- Exclude the common columns between the tables,
--  to be called with dbt_utils.star (instead of using
--  the * operator to select all columns).


select
	patient_id_cp,
	event_date as item_event_date,
	rx_number,
	invoice_number,
	
  
,
	
from "datawarehouse".prod_analytics."order_items_historic" oih