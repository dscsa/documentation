with orders as (
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
        cast(
            jsonb_extract_path_text(_airbyte_data, 'order_date_expected_initial') as timestamp
        ) as order_date_expected_initial,
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
        cast(jsonb_extract_path_text(_airbyte_data, 'payment_default_updated_at') as timestamp) as payment_default_updated_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'payment_actual_updated_at') as timestamp) as payment_actual_updated_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'order_stage_wc_updated_at') as timestamp) as order_stage_wc_updated_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'payment_date_autopay') as varchar(80)) as payment_date_autopay,
        cast(jsonb_extract_path_text(_airbyte_data, 'payment_method_actual') as varchar(80)) as payment_method_actual,
        cast(jsonb_extract_path_text(_airbyte_data, 'order_payment_coupon') as varchar(255)) as order_payment_coupon,
        cast(jsonb_extract_path_text(_airbyte_data, 'order_note') as varchar(255)) as order_note,
        cast(jsonb_extract_path_text(_airbyte_data, 'priority') as int) as priority,
        cast(jsonb_extract_path_text(_airbyte_data, 'tech_fill') as varchar(5)) as tech_fill,
        cast(jsonb_extract_path_text(_airbyte_data, 'rph_check') as varchar(5)) as rph_check,
        cast(jsonb_extract_path_text(_airbyte_data, 'add_user_id') as int) as add_user_id,
        cast(jsonb_extract_path_text(_airbyte_data, 'add_user_id') as int) as chg_user_id,
        cast(jsonb_extract_path_text(_airbyte_data, 'shipping_speed') as int) as shipping_speed,
        cast(jsonb_extract_path_text(_airbyte_data, 'updated_at') as timestamp) as updated_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'created_at') as timestamp) as created_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'rx_group_removals_checked_at') as timestamp) as rx_group_removals_checked_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'rx_group_additions_checked_at') as timestamp) as rx_group_additions_checked_at
    from
        "datawarehouse"."raw"._airbyte_raw_goodpill_gp_orders
)

select
    invoice_number,
    patient_id_cp,
    patient_id_wc,
    count_items,
    count_filled,
    count_nofill,
    priority,
    nullif(order_source, '') as order_source,
    nullif(order_stage_cp, '') as order_stage_cp,
    nullif(order_stage_wc, '') as order_stage_wc,
    nullif(order_status, '') as order_status,
    nullif(order_address1, '') as order_address1,
    nullif(order_address2, '') as order_address2,
    nullif(invoice_doc_id, '') as invoice_doc_id,
    nullif(tracking_number, '') as tracking_number,
    payment_total_default,
    payment_total_actual,
    payment_fee_default,
    payment_fee_actual,
    payment_due_default,
    payment_due_actual,
    payment_default_updated_at,
    payment_actual_updated_at,
    nullif(payment_date_autopay, '') as payment_date_autopay,
    nullif(payment_method_actual, '') as payment_method_actual,
    nullif(order_payment_coupon, '') as order_payment_coupon,
    nullif(order_note, '') as order_note,
    nullif(rph_check, '') as rph_check,
    nullif(tech_fill, '') as tech_fill,
    order_date_returned,
    order_date_shipped,
    order_date_dispensed,
    order_date_added,
    order_date_changed,
    order_date_delivered,
    order_date_expedited,
    order_date_expected,
    order_date_expected_initial,
    order_date_failed,
    order_date_updated,
    order_stage_wc_updated_at,
    nullif(order_city, '') as order_city,
    nullif(order_state, '') as order_state,
    nullif(order_zip, '') as order_zip,
    add_user_id,
    chg_user_id,
    shipping_speed,
    created_at,
    updated_at,
    _airbyte_emitted_at,
    _airbyte_ab_id,
    rx_group_additions_checked_at,
    rx_group_removals_checked_at
from orders