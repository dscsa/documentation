-- Exclude the common columns between the tables,
--  to be called with dbt_utils.star (instead of using
--  the * operator to select all columns).


select
	patient_id_cp,
	event_date as order_event_date,
	invoice_number,
	
  
,
	
from "datawarehouse".prod_analytics."orders_historic" oh