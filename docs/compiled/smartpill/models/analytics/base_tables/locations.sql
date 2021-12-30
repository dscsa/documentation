
WITH  __dbt__cte__gp_orders as (
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
    cast(jsonb_extract_path_text(_airbyte_data, 'coupon_lines') as varchar(255)) as coupon_lines,
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
),  __dbt__cte__gp_patients as (
select
    _airbyte_emitted_at,
    _airbyte_ab_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_id_cp') as int) as patient_id_cp,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_id_wc') as int) as patient_id_wc,
    cast(jsonb_extract_path_text(_airbyte_data, 'first_name') as varchar(80)) as first_name,
    cast(jsonb_extract_path_text(_airbyte_data, 'last_name') as varchar(80)) as last_name,
    cast(jsonb_extract_path_text(_airbyte_data, 'birth_date') as timestamp) as birth_date,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_note') as varchar(3072)) as patient_note,
    cast(jsonb_extract_path_text(_airbyte_data, 'phone1') as varchar(10)) as phone1,
    cast(jsonb_extract_path_text(_airbyte_data, 'phone2') as varchar(10)) as phone2,
    cast(jsonb_extract_path_text(_airbyte_data, 'email') as varchar(255)) as email,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_autofill') as int) as patient_autofill,
    cast(jsonb_extract_path_text(_airbyte_data, 'pharmacy_name') as varchar(50)) as pharmacy_name,
    cast(jsonb_extract_path_text(_airbyte_data, 'pharmacy_npi') as varchar(10)) as pharmacy_npi,
    cast(jsonb_extract_path_text(_airbyte_data, 'pharmacy_fax') as varchar(12)) as pharmacy_fax,
    cast(jsonb_extract_path_text(_airbyte_data, 'pharmacy_phone') as varchar(12)) as pharmacy_phone,
    cast(jsonb_extract_path_text(_airbyte_data, 'pharmacy_address') as varchar(255)) as pharmacy_address,
    cast(jsonb_extract_path_text(_airbyte_data, 'payment_card_type') as varchar(20)) as payment_card_type,
    cast(jsonb_extract_path_text(_airbyte_data, 'payment_card_last4') as varchar(4)) as payment_card_last4,
    cast(jsonb_extract_path_text(_airbyte_data, 'payment_card_date_expired') as timestamp) as payment_card_date_expired,
    cast(jsonb_extract_path_text(_airbyte_data, 'payment_method_default') as varchar(50)) as payment_method_default,
    cast(jsonb_extract_path_text(_airbyte_data, 'payment_coupon') as varchar(20)) as payment_coupon,
    cast(jsonb_extract_path_text(_airbyte_data, 'tracking_coupon') as varchar(20)) as tracking_coupon,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_address1') as varchar(255)) as patient_address1,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_address2') as varchar(255)) as patient_address2,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_city') as varchar(255)) as patient_city,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_state') as varchar(2)) as patient_state,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_zip') as varchar(5)) as patient_zip,
    cast(jsonb_extract_path_text(_airbyte_data, 'refills_used') as decimal(5,2)) as refills_used,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_status') as int) as patient_status,
    cast(jsonb_extract_path_text(_airbyte_data, 'language') as int) as language,
    cast(jsonb_extract_path_text(_airbyte_data, 'allergies_none') as varchar(80)) as allergies_none,
    cast(jsonb_extract_path_text(_airbyte_data, 'allergies_cephalosporins') as varchar(80)) as allergies_cephalosporins,
    cast(jsonb_extract_path_text(_airbyte_data, 'allergies_sulfa') as varchar(80)) as allergies_sulfa,
    cast(jsonb_extract_path_text(_airbyte_data, 'allergies_aspirin') as varchar(80)) as allergies_aspirin,
    cast(jsonb_extract_path_text(_airbyte_data, 'allergies_penicillin') as varchar(80)) as allergies_penicillin,
    cast(jsonb_extract_path_text(_airbyte_data, 'allergies_erythromycin') as varchar(80)) as allergies_erythromycin,
    cast(jsonb_extract_path_text(_airbyte_data, 'allergies_codeine') as varchar(80)) as allergies_codeine,
    cast(jsonb_extract_path_text(_airbyte_data, 'allergies_nsaids') as varchar(80)) as allergies_nsaids,
    cast(jsonb_extract_path_text(_airbyte_data, 'allergies_salicylates') as varchar(80)) as allergies_salicylates,
    cast(jsonb_extract_path_text(_airbyte_data, 'allergies_azithromycin') as varchar(80)) as allergies_azithromycin,
    cast(jsonb_extract_path_text(_airbyte_data, 'allergies_amoxicillin') as varchar(80)) as allergies_amoxicillin,
    cast(jsonb_extract_path_text(_airbyte_data, 'allergies_tetracycline') as varchar(80)) as allergies_tetracycline,
    cast(jsonb_extract_path_text(_airbyte_data, 'allergies_other') as varchar(255)) as allergies_other,
    cast(jsonb_extract_path_text(_airbyte_data, 'medications_other') as varchar(3072)) as medications_other,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_date_added') as timestamp) as patient_date_added,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_date_registered') as timestamp) as patient_date_registered,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_date_changed') as timestamp) as patient_date_changed,
    cast(jsonb_extract_path_text(_airbyte_data, 'patient_date_updated') as timestamp) as patient_date_updated,
    cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_updated_at') as timestamp) as _ab_cdc_updated_at,
    cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_deleted_at') as timestamp) as _ab_cdc_deleted_at
from
    "datawarehouse".raw._airbyte_raw_goodpill_gp_patients
),locations AS (
    select 
        order_city as city, 
        order_state as state, 
        order_zip as zip_code,
        _airbyte_emitted_at
    from __dbt__cte__gp_orders
    UNION
    select 
       patient_city as city,
       patient_state as state,
       patient_zip as zip_code,
       _airbyte_emitted_at
    from __dbt__cte__gp_patients 
)
select distinct on (zip_code) 
    cast(city as varchar(255)) as city, 
    cast(state as varchar(255)) as state, 
    cast(zip_code as varchar(255)) as zip_code, 
    NOW() as date_processed
FROM locations 
WHERE zip_code is not null

    and _airbyte_emitted_at > (select MAX(date_processed) from "datawarehouse".analytics."locations")
