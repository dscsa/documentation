

-- depends_on: __dbt__cte__gp_orders

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

	where event_date > (select MAX(event_date) from "datawarehouse".dev_analytics."orders_historic")

order by invoice_number, event_name, event_date desc