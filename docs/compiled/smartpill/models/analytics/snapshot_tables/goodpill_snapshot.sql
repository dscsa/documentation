

-- Exclude the common columns between the tables,
--  to be called with dbt_utils.star (instead of using
--  the * operator to select all columns).



with psh as (
	select distinct on (patient_id_cp)
		patient_id_cp,
		event_date as patient_event_date,
		event_name as patient_event_name,
		
  

	from "datawarehouse".prod_analytics."patients_status_historic"
	order by patient_id_cp, patient_event_date desc
	/* inner join "datawarehouse".prod_analytics."goodpill_events" using (event_name) */
),

rh as (
	select
		rh.patient_id_cp,
		event_date as rx_event_date,
		rh.rx_number,
		
  
,
		
	from "datawarehouse".prod_analytics."rxs_historic" rh
	/* inner join "datawarehouse".prod_analytics."goodpill_events" using (event_name) */
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
		
  
,
		
	from "datawarehouse".prod_analytics."order_items_historic" oih
	/* inner join "datawarehouse".prod_analytics."goodpill_events" using (event_name) */
),

oh as (
	select
		patient_id_cp,
		event_date as order_event_date,
		invoice_number,
		
  
,
		
	from "datawarehouse".prod_analytics."orders_historic" oh
	/* inner join "datawarehouse".prod_analytics."goodpill_events" using (event_name) */
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