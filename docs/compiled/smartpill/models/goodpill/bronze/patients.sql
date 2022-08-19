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
        cast(jsonb_extract_path_text(_airbyte_data, 'payment_card_autopay') as int) as payment_card_autopay,
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
        cast(jsonb_extract_path_text(_airbyte_data, 'initial_invoice_number') as int) as initial_invoice_number,
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

select distinct on (patients.patient_id_cp)
    patients.patient_id_cp,
    patients.patient_date_registered,
    patients.patient_date_added,
    patients.patient_date_changed,
    nullif(patients.first_name, '') as first_name,
    nullif(patients.last_name, '') as last_name,
    patients.birth_date,
    nullif(patients.language, '') as "language",
    nullif(patients.phone1, '') as phone1,
    nullif(patients.phone2, '') as phone2,
    concat(patients.patient_address1, ', ', patients.patient_address2) as patient_address,
    nullif(patients.patient_city, '') as patient_city,
    nullif(patients.patient_state, '') as patient_state,
    nullif(patients.patient_zip, '') as patient_zip,
    nullif(patients.payment_card_type, '') as payment_card_type,
    nullif(patients.payment_card_last4, '') as payment_card_last4,
    patients.payment_card_date_expired,
    patients.payment_card_autopay,
    nullif(patients.payment_method_default, '') as payment_method_default,
    cmc.clinic_id as clinic_id_coupon,
    nullif(patients.payment_coupon, '') as payment_coupon,
    nullif(patients.tracking_coupon, '') as tracking_coupon,
    patients.patient_date_first_rx_received,
    patients.patient_date_first_dispensed,
    patients.patient_date_first_expected_by,
    patients.refills_used,
    nullif(patients.pharmacy_npi, '') as pharmacy_npi,
    nullif(patients.pharmacy_name, '') as pharmacy_name,
    nullif(patients.pharmacy_phone, '') as pharmacy_phone,
    nullif(patients.pharmacy_fax, '') as pharmacy_fax,
    nullif(patients.pharmacy_address, '') as pharmacy_address,
    nullif(patients.patient_inactive, '') as patient_inactive,
    patients.patient_id_wc,
    nullif(patients.email, '') as email,
    patients.patient_autofill,
    nullif(patients.patient_note, '') as patient_note,
    patients.initial_invoice_number,
    nullif(patients.allergies_none, '') as allergies_none,
    nullif(patients.allergies_cephalosporins, '') as allergies_cephalosporins,
    nullif(patients.allergies_sulfa, '') as allergies_sulfa,
    nullif(patients.allergies_aspirin, '') as allergies_aspirin,
    nullif(patients.allergies_penicillin, '') as allergies_penicillin,
    nullif(patients.allergies_erythromycin, '') as allergies_erythromycin,
    nullif(patients.allergies_codeine, '') as allergies_codeine,
    nullif(patients.allergies_nsaids, '') as allergies_nsaids,
    nullif(patients.allergies_salicylates, '') as allergies_salicylates,
    nullif(patients.allergies_azithromycin, '') as allergies_azithromycin,
    nullif(patients.allergies_amoxicillin, '') as allergies_amoxicillin,
    nullif(patients.allergies_tetracycline, '') as allergies_tetracycline,
    nullif(patients.allergies_other, '') as allergies_other,
    nullif(patients.medications_other, '') as medications_other,
    patients.patient_date_updated,
    now() as date_processed
from patients
left join
    "datawarehouse".dev_analytics."clinic_coupons" as cmc on
        patients.payment_coupon = cmc.coupon_code or patients.tracking_coupon = cmc.coupon_code
where
    lower(patients.first_name) not like '%test%'
    and lower(patients.first_name) not like '%user%'
    and lower(patients.last_name) not like '%test%'
    and lower(patients.last_name) not like '%user%'

order by patients.patient_id_cp, patients.patient_date_updated desc