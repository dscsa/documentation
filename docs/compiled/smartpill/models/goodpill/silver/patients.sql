with patients as (
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
        cast(
            jsonb_extract_path_text(_airbyte_data, 'payment_card_date_expired') as timestamp
        ) as payment_card_date_expired,
        cast(jsonb_extract_path_text(_airbyte_data, 'payment_method_default') as varchar(50)) as payment_method_default,
        cast(jsonb_extract_path_text(_airbyte_data, 'payment_coupon') as varchar(20)) as payment_coupon,
        cast(jsonb_extract_path_text(_airbyte_data, 'tracking_coupon') as varchar(20)) as tracking_coupon,
        cast(jsonb_extract_path_text(_airbyte_data, 'patient_address1') as varchar(255)) as patient_address1,
        cast(jsonb_extract_path_text(_airbyte_data, 'patient_address2') as varchar(255)) as patient_address2,
        cast(jsonb_extract_path_text(_airbyte_data, 'patient_city') as varchar(255)) as patient_city,
        cast(jsonb_extract_path_text(_airbyte_data, 'patient_state') as varchar(2)) as patient_state,
        cast(jsonb_extract_path_text(_airbyte_data, 'patient_zip') as varchar(5)) as patient_zip,
        cast(jsonb_extract_path_text(_airbyte_data, 'refills_used') as decimal(5, 2)) as refills_used,
        cast(jsonb_extract_path_text(_airbyte_data, 'patient_status') as int) as patient_status,
        cast(jsonb_extract_path_text(_airbyte_data, 'language') as varchar) as "language",
        cast(jsonb_extract_path_text(_airbyte_data, 'allergies_none') as varchar(80)) as allergies_none,
        cast(
            jsonb_extract_path_text(_airbyte_data, 'allergies_cephalosporins') as varchar(80)
        ) as allergies_cephalosporins,
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
        cast(jsonb_extract_path_text(_airbyte_data, 'patient_inactive') as varchar) as patient_inactive,
        cast(
            jsonb_extract_path_text(_airbyte_data, 'patient_date_first_dispensed') as timestamp
        ) as patient_date_first_dispensed,
        cast(
            jsonb_extract_path_text(_airbyte_data, 'patient_date_first_rx_received') as timestamp
        ) as patient_date_first_rx_received,
        cast(
            jsonb_extract_path_text(_airbyte_data, 'patient_date_first_expected_by') as timestamp
        ) as patient_date_first_expected_by,
        cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_updated_at') as timestamp) as _ab_cdc_updated_at,
        cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_deleted_at') as timestamp) as _ab_cdc_deleted_at
    from
        "datawarehouse"."raw"._airbyte_raw_goodpill_gp_patients
)

select distinct on (patient_id_cp)
    patient_id_cp,
    patient_date_registered,
    patient_date_added,
    patient_date_changed,
    first_name,
    last_name,
    birth_date,
    phone1,
    phone2,
    concat(patient_address1, ', ', patient_address2) as patient_address,
    patient_city,
    patient_state,
    patient_zip,
    payment_card_type,
    payment_card_last4,
    payment_card_date_expired,
    payment_method_default,
    clinic_name as clinic_name_coupon,
    payment_coupon,
    tracking_coupon,
    patient_date_first_rx_received as date_first_rx_received,
    patient_date_first_dispensed as date_first_dispensed,
    patient_date_first_expected_by as date_first_expected_by,
    refills_used,
    pharmacy_npi,
    pharmacy_name,
    pharmacy_phone,
    pharmacy_fax,
    pharmacy_address,
    patient_inactive,
    patient_date_updated,
    now() as date_processed
from patients
left join "datawarehouse".dev_analytics."clinics_meta_coupons" as cmc on patients.payment_coupon = cmc.coupon_code or patients.tracking_coupon = cmc.coupon_code
where
    lower(first_name) not like '%test%'
    and lower(first_name) not like '%user%'
    and lower(last_name) not like '%test%'
    and lower(last_name) not like '%user%'

order by patient_id_cp, patient_date_updated desc