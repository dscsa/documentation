

-- depends_on: __dbt__cte__gp_orders
-- depends_on: __dbt__cte__an_orders_logs
-- depends_on: __dbt__cte__an_patients
-- depends_on: __dbt__cte__an_locations

with  __dbt__cte__gp_orders as (
select
    _airbyte_emitted_at,
    _airbyte_ab_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'invoice_number') as int) as invoice_number,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_id_cp') as int) as patient_id_cp,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_id_wc') as int) as patient_id_wc,
    cast(jsonb_extract_path_text(_airbyte_data, 'count_items') as int) as count_items,
    cast(jsonb_extract_path_text(_airbyte_data, 'count_filled') as int) as count_filled,
    cast(jsonb_extract_path_text(_airbyte_data, 'count_nofill') as int) as count_nofill,
    cast(jsonb_extract_path_text(_airbyte_data, 'order_source') as varchar(80)) as order_source,
    cast(jsonb_extract_path_text(_airbyte_data, 'order_stage_cp') as varchar(80)) as order_stage_cp,
    cast(jsonb_extract_path_text(_airbyte_data, 'order_stage_wc') as varchar(80)) as order_stage_wc,
    cast(jsonb_extract_path_text(_airbyte_data, 'order_status') as varchar(80)) as order_status,
    cast(jsonb_extract_path_text(_airbyte_data, 'invoice_doc_id') as varchar(80)) as invoice_doc_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'order_address1') as varchar(255)) as order_address1,
    cast(jsonb_extract_path_text(_airbyte_data, 'order_address2') as varchar(255)) as order_address2,
    cast(jsonb_extract_path_text(_airbyte_data, 'order_city') as varchar(255)) as order_city,
    cast(jsonb_extract_path_text(_airbyte_data, 'order_state') as varchar(2)) as order_state,
    cast(jsonb_extract_path_text(_airbyte_data, 'order_zip') as varchar(5)) as order_zip,
    cast(jsonb_extract_path_text(_airbyte_data, 'tracking_number') as varchar(80)) as tracking_number,
    cast(jsonb_extract_path_text(_airbyte_data, 'order_date_added') as timestamp) as order_date_added,
    cast(jsonb_extract_path_text(_airbyte_data, 'order_date_changed') as timestamp) as order_date_changed,
    cast(jsonb_extract_path_text(_airbyte_data, 'order_date_updated') as timestamp) as order_date_updated,
    cast(jsonb_extract_path_text(_airbyte_data, 'order_date_expedited') as timestamp) as order_date_expedited,
    cast(jsonb_extract_path_text(_airbyte_data, 'order_date_expected') as timestamp) as order_date_expected,
    cast(jsonb_extract_path_text(_airbyte_data, 'order_date_dispensed') as timestamp) as order_date_dispensed,
    cast(jsonb_extract_path_text(_airbyte_data, 'order_date_shipped') as timestamp) as order_date_shipped,
    cast(jsonb_extract_path_text(_airbyte_data, 'order_date_delivered') as timestamp) as order_date_delivered,
    cast(jsonb_extract_path_text(_airbyte_data, 'order_date_returned') as timestamp) as order_date_returned,
    cast(jsonb_extract_path_text(_airbyte_data, 'order_date_failed') as timestamp) as order_date_failed,
    cast(jsonb_extract_path_text(_airbyte_data, 'payment_total_default') as int) as payment_total_default,
    cast(jsonb_extract_path_text(_airbyte_data, 'payment_total_actual') as int) as payment_total_actual,
    cast(jsonb_extract_path_text(_airbyte_data, 'payment_fee_default') as int) as payment_fee_default,
    cast(jsonb_extract_path_text(_airbyte_data, 'payment_fee_actual') as int) as payment_fee_actual,
    cast(jsonb_extract_path_text(_airbyte_data, 'payment_due_default') as int) as payment_due_default,
    cast(jsonb_extract_path_text(_airbyte_data, 'payment_due_actual') as int) as payment_due_actual,
    cast(jsonb_extract_path_text(_airbyte_data, 'payment_date_autopay') as varchar(80)) as payment_date_autopay,
    cast(jsonb_extract_path_text(_airbyte_data, 'payment_method_actual') as varchar(80)) as payment_method_actual,
    cast(jsonb_extract_path_text(_airbyte_data, 'order_payment_coupon') as varchar(255)) as order_payment_coupon,
    cast(jsonb_extract_path_text(_airbyte_data, 'order_note') as varchar(255)) as order_note,
    cast(jsonb_extract_path_text(_airbyte_data, 'priority') as int) as priority,
    cast(jsonb_extract_path_text(_airbyte_data, 'tech_fill') as varchar(5)) as tech_fill,
    cast(jsonb_extract_path_text(_airbyte_data, 'rph_check') as varchar(5)) as rph_check,
    cast(jsonb_extract_path_text(_airbyte_data, 'updated_at') as timestamp) as updated_at,
    cast(jsonb_extract_path_text(_airbyte_data, 'created_at') as timestamp) as created_at,
    cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_updated_at') as timestamp) as _ab_cdc_updated_at,
    cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_deleted_at') as timestamp) as _ab_cdc_deleted_at
from
    "datawarehouse".raw._airbyte_raw_goodpill_gp_orders
),  __dbt__cte__an_orders_logs as (
select
	_airbyte_ab_id,
	_airbyte_emitted_at,
	cast(jsonb_extract_path_text(_airbyte_data, 'invoice_number') as int) as invoice_number,
	cast(jsonb_extract_path_text(_airbyte_data, 'patient_id') as int) as patient_id,
	cast(jsonb_extract_path_text(_airbyte_data, 'event_date_id') as int) as event_date_id,
	cast(jsonb_extract_path_text(_airbyte_data, 'event_time_id') as int) as event_time_id,
	cast(jsonb_extract_path_text(_airbyte_data, 'location_id') as int) as location_id,
	cast(jsonb_extract_path_text(_airbyte_data, 'event_type') as varchar(15)) as event_type,
	cast(jsonb_extract_path_text(_airbyte_data, 'goodpill_event_date') as timestamp) as goodpill_event_date,
	cast(jsonb_extract_path_text(_airbyte_data, 'count_items') as int) as count_items,
	cast(jsonb_extract_path_text(_airbyte_data, 'count_filled') as int) as count_filled,
	cast(jsonb_extract_path_text(_airbyte_data, 'count_nofill') as int) as count_nofill,
	cast(jsonb_extract_path_text(_airbyte_data, 'order_source') as varchar(80)) as order_source,
	cast(jsonb_extract_path_text(_airbyte_data, 'order_stage_cp') as varchar(80)) as order_stage_cp,
	cast(jsonb_extract_path_text(_airbyte_data, 'order_status') as varchar(80)) as order_status,
	cast(jsonb_extract_path_text(_airbyte_data, 'invoice_doc_id') as varchar(80)) as invoice_doc_id,
	cast(jsonb_extract_path_text(_airbyte_data, 'tracking_number') as varchar(80)) as tracking_number,
	cast(jsonb_extract_path_text(_airbyte_data, 'order_date_changed') as timestamp) as order_date_changed,
	cast(jsonb_extract_path_text(_airbyte_data, 'order_date_updated') as timestamp) as order_date_updated,
	cast(jsonb_extract_path_text(_airbyte_data, 'payment_total_default') as int) as payment_total_default,
	cast(jsonb_extract_path_text(_airbyte_data, 'payment_total_actual') as int) as payment_total_actual,
	cast(jsonb_extract_path_text(_airbyte_data, 'payment_fee_default') as int) as payment_fee_default,
	cast(jsonb_extract_path_text(_airbyte_data, 'payment_fee_actual') as int) as payment_fee_actual,
	cast(jsonb_extract_path_text(_airbyte_data, 'payment_due_default') as int) as payment_due_default,
	cast(jsonb_extract_path_text(_airbyte_data, 'payment_due_actual') as int) as payment_due_actual,
	cast(jsonb_extract_path_text(_airbyte_data, 'payment_date_autopay') as varchar(80)) as payment_date_autopay,
	cast(jsonb_extract_path_text(_airbyte_data, 'payment_method_actual') as varchar(80)) as payment_method_actual,
	cast(jsonb_extract_path_text(_airbyte_data, 'coupon_lines') as varchar(255)) as coupon_lines,
	cast(jsonb_extract_path_text(_airbyte_data, 'order_note') as text) as order_note,
	cast(jsonb_extract_path_text(_airbyte_data, 'rph_check') as varchar(5)) as rph_check,
	cast(jsonb_extract_path_text(_airbyte_data, 'tech_fill') as varchar(5)) as tech_fill,
	cast(jsonb_extract_path_text(_airbyte_data, 'date_processed') as timestamp) as date_processed
from "datawarehouse".raw._airbyte_raw_analytics_orders_logs
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
),  __dbt__cte__an_locations as (
select
	_airbyte_ab_id,
	_airbyte_emitted_at,
	cast(jsonb_extract_path_text(_airbyte_data, 'location_id') as int) as location_id,
	cast(jsonb_extract_path_text(_airbyte_data, 'city') as varchar(255)) as city,
	cast(jsonb_extract_path_text(_airbyte_data, 'state') as varchar(255)) as state,
	cast(jsonb_extract_path_text(_airbyte_data, 'zip_code') as varchar(255)) as zip_code,
	cast(jsonb_extract_path_text(_airbyte_data, 'date_processed') as timestamp) as date_processed
from "datawarehouse".raw._airbyte_raw_analytics_locations
),oe as (
	
		select
			*,
			'ORDER_DELETED' as event_name,
			_ab_cdc_deleted_at as event_date,
			'GOODPILL' as _airbyte_source
		from __dbt__cte__gp_orders
		where _ab_cdc_deleted_at is not null
		union
		select
			*,
			'ORDER_RETURNED' as event_name,
			order_date_returned as event_date,
			'GOODPILL' as _airbyte_source
		from __dbt__cte__gp_orders
		where order_date_returned is not null
		union
		select
			*,
			'ORDER_SHIPPED' as event_name,
			order_date_shipped as event_date,
			'GOODPILL' as _airbyte_source
		from __dbt__cte__gp_orders
		where order_date_shipped is not null
		union
		select
			*,
			'ORDER_DISPENSED' as event_name,
			order_date_dispensed as event_date,
			'GOODPILL' as _airbyte_source
		from __dbt__cte__gp_orders
		where order_date_dispensed is not null
		union
		select
			*,
			'ORDER_ADDED' as event_name,
			order_date_added as event_date,
			'GOODPILL' as _airbyte_source
		from __dbt__cte__gp_orders
		where order_date_added is not null
	
)
select distinct on (invoice_number, event_name)
	invoice_number,
	event_name,
	event_date,
	patient_id_cp,
	coalesce(order_zip, order_state) as location_id,
	count_items,
	count_filled,
	count_nofill,
	order_source,
	order_stage_cp,
	order_status,
	invoice_doc_id,
	tracking_number,
	payment_total_default,
	payment_total_actual,
	payment_fee_default,
	payment_fee_actual,
	payment_due_default,
	payment_due_actual,
	payment_date_autopay,
	payment_method_actual,
	order_payment_coupon,
	order_note,
	rph_check,
	tech_fill,
	_airbyte_emitted_at,
	_airbyte_ab_id,
	_ab_cdc_updated_at,
	_airbyte_source,
	md5(cast(event_name || invoice_number as 
    varchar
)) as unique_event_id
from oe

	where event_date > (select MAX(event_date) from "datawarehouse".prod_analytics."orders_historic")

order by invoice_number, event_name, event_date desc