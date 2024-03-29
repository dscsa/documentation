with order_items as (
    select
        _airbyte_emitted_at,
        _airbyte_ab_id,
        cast(jsonb_extract_path_text(_airbyte_data, 'invoice_number') as int) as invoice_number,
        cast(jsonb_extract_path_text(_airbyte_data, 'patient_id_cp') as int) as patient_id_cp,
        cast(jsonb_extract_path_text(_airbyte_data, 'line_id') as int) as line_id,
        cast(jsonb_extract_path_text(_airbyte_data, 'rx_number') as int) as rx_number,
        cast(jsonb_extract_path_text(_airbyte_data, 'groups') as varchar(255)) as groups,
        cast(jsonb_extract_path_text(_airbyte_data, 'rx_dispensed_id') as int) as rx_dispensed_id,
        cast(jsonb_extract_path_text(_airbyte_data, 'stock_level_initial') as varchar(80)) as stock_level_initial,
        cast(
            jsonb_extract_path_text(_airbyte_data, 'rx_message_keys_initial') as varchar(255)
        ) as rx_message_keys_initial,
        cast(jsonb_extract_path_text(_airbyte_data, 'patient_autofill_initial') as int) as patient_autofill_initial,
        cast(jsonb_extract_path_text(_airbyte_data, 'rx_autofill_initial') as int) as rx_autofill_initial,
        cast(jsonb_extract_path_text(_airbyte_data, 'rx_numbers_initial') as varchar(255)) as rx_numbers_initial,
        cast(jsonb_extract_path_text(_airbyte_data, 'zscore_initial') as decimal(6, 3)) as zscore_initial,
        cast(
            jsonb_extract_path_text(_airbyte_data, 'refills_dispensed_default') as decimal(5, 2)
        ) as refills_dispensed_default,
        cast(
            jsonb_extract_path_text(_airbyte_data, 'refills_dispensed_actual') as decimal(5, 2)
        ) as refills_dispensed_actual,
        cast(jsonb_extract_path_text(_airbyte_data, 'days_dispensed_default') as int) as days_dispensed_default,
        cast(jsonb_extract_path_text(_airbyte_data, 'days_dispensed_actual') as int) as days_dispensed_actual,
        cast(
            jsonb_extract_path_text(_airbyte_data, 'qty_dispensed_default') as decimal(10, 3)
        ) as qty_dispensed_default,
        cast(jsonb_extract_path_text(_airbyte_data, 'qty_dispensed_actual') as decimal(10, 3)) as qty_dispensed_actual,
        cast(
            jsonb_extract_path_text(_airbyte_data, 'price_dispensed_default') as decimal(5, 2)
        ) as price_dispensed_default,
        cast(
            jsonb_extract_path_text(_airbyte_data, 'price_dispensed_actual') as decimal(5, 2)
        ) as price_dispensed_actual,
        cast(
            jsonb_extract_path_text(_airbyte_data, 'unit_price_retail_initial') as decimal(13, 4)
        ) as unit_price_retail_initial,
        cast(
            jsonb_extract_path_text(_airbyte_data, 'unit_price_goodrx_initial') as decimal(13, 4)
        ) as unit_price_goodrx_initial,
        cast(
            jsonb_extract_path_text(_airbyte_data, 'unit_price_nadac_initial') as decimal(13, 4)
        ) as unit_price_nadac_initial,
        cast(
            jsonb_extract_path_text(_airbyte_data, 'unit_price_awp_initial') as decimal(13, 4)
        ) as unit_price_awp_initial,
        cast(jsonb_extract_path_text(_airbyte_data, 'qty_pended_total') as decimal(10, 3)) as qty_pended_total,
        cast(jsonb_extract_path_text(_airbyte_data, 'qty_pended_repacks') as decimal(10, 3)) as qty_pended_repacks,
        cast(jsonb_extract_path_text(_airbyte_data, 'count_pended_total') as int) as count_pended_total,
        cast(jsonb_extract_path_text(_airbyte_data, 'count_pended_repacks') as int) as count_pended_repacks,
        cast(jsonb_extract_path_text(_airbyte_data, 'item_message_keys') as varchar(255)) as item_message_keys,
        cast(jsonb_extract_path_text(_airbyte_data, 'item_message_text') as varchar(255)) as item_message_text,
        cast(jsonb_extract_path_text(_airbyte_data, 'item_type') as varchar(80)) as item_type,
        cast(jsonb_extract_path_text(_airbyte_data, 'item_added_by') as varchar(80)) as item_added_by,
        cast(jsonb_extract_path_text(_airbyte_data, 'item_date_added') as timestamp) as item_date_added,
        cast(jsonb_extract_path_text(_airbyte_data, 'item_date_changed') as timestamp) as item_date_changed,
        cast(jsonb_extract_path_text(_airbyte_data, 'refill_date_last') as timestamp) as refill_date_last,
        cast(jsonb_extract_path_text(_airbyte_data, 'refill_date_manual') as timestamp) as refill_date_manual,
        cast(jsonb_extract_path_text(_airbyte_data, 'refill_date_default') as timestamp) as refill_date_default,
        cast(jsonb_extract_path_text(_airbyte_data, 'add_user_id') as int) as add_user_id,
        cast(jsonb_extract_path_text(_airbyte_data, 'chg_user_id') as int) as chg_user_id,
        cast(jsonb_extract_path_text(_airbyte_data, 'count_lines') as int) as count_lines,
        cast(jsonb_extract_path_text(_airbyte_data, 'drug_generic_pended') as varchar(255)) as drug_generic_pended,
        cast(jsonb_extract_path_text(_airbyte_data, 'drug_name') as varchar(255)) as drug_name,
        cast(jsonb_extract_path_text(_airbyte_data, 'repacked_by') as varchar(5)) as repacked_by,
        cast(jsonb_extract_path_text(_airbyte_data, 'created_at') as timestamp) as created_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'updated_at') as timestamp) as updated_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'days_and_message_initial_at') as timestamp) as days_and_message_initial_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'days_and_message_updated_at') as timestamp) as days_and_message_updated_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'days_pended') as int) as days_pended,
        cast(jsonb_extract_path_text(_airbyte_data, 'qty_per_day_pended') as decimal(6,3)) as qty_per_day_pended,
        cast(jsonb_extract_path_text(_airbyte_data, 'unpended_at') as timestamp) as unpended_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'pend_initial_at') as timestamp) as pend_initial_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'pend_updated_at') as timestamp) as pend_updated_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'ndc_pended') as varchar(255)) as ndc_pended,
        cast(jsonb_extract_path_text(_airbyte_data, 'filled_at') as timestamp) as filled_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'pend_failed_at') as timestamp) as pend_failed_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'filled_by') as bigint) as filled_by,
        cast(jsonb_extract_path_text(_airbyte_data, 'pend_retried_by') as bigint) as pend_retried_by,
        cast(jsonb_extract_path_text(_airbyte_data, 'status') as varchar) as status,
        cast(jsonb_extract_path_text(_airbyte_data, 'pend_retried_days') as integer) as pend_retried_days,
        cast(jsonb_extract_path_text(_airbyte_data, 'pend_retried_at') as timestamp) as pend_retried_at
    from
        "datawarehouse"."raw"._airbyte_raw_goodpill_gp_order_items
)

select
    invoice_number,
    patient_id_cp,
    rx_number,
    line_id,
    nullif(groups, '') as groups,
    rx_dispensed_id,
    nullif(stock_level_initial, '') as stock_level_initial,
    nullif(rx_message_keys_initial, '') as rx_message_keys_initial,
    patient_autofill_initial,
    rx_autofill_initial,
    nullif(rx_numbers_initial, '') as rx_numbers_initial,
    zscore_initial,
    refills_dispensed_default,
    refills_dispensed_actual,
    days_dispensed_default,
    days_dispensed_actual,
    qty_dispensed_default,
    qty_dispensed_actual,
    price_dispensed_default,
    price_dispensed_actual,
    unit_price_retail_initial,
    unit_price_goodrx_initial,
    unit_price_nadac_initial,
    unit_price_awp_initial,
    qty_pended_total,
    qty_pended_repacks,
    count_pended_total,
    count_pended_repacks,
    nullif(item_message_keys, '') as item_message_keys,
    nullif(item_message_text, '') as item_message_text,
    nullif(item_type, '') as item_type,
    nullif(item_added_by, '') as item_added_by,
    item_date_added,
    item_date_changed,
    refill_date_last,
    refill_date_manual,
    refill_date_default,
    add_user_id,
    chg_user_id,
    count_lines,
    drug_generic_pended,
    drug_name,
    nullif(repacked_by, '') as repacked_by,
    updated_at,
    created_at,
    days_and_message_updated_at,
    days_and_message_initial_at,
    days_pended,
    qty_per_day_pended,
    unpended_at,
    pend_initial_at,
    pend_updated_at,
    _airbyte_emitted_at,
    _airbyte_ab_id,
    ndc_pended,
    filled_at,
    pend_failed_at,
    filled_by,
    pend_retried_by,
    status,
    pend_retried_days,
    pend_retried_at
from order_items