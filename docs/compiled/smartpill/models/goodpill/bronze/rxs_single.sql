

with rxs_single as (
    select
        _airbyte_emitted_at,
        _airbyte_ab_id,
        cast(jsonb_extract_path_text(_airbyte_data, 'rx_number') as int) as rx_number,
        cast(jsonb_extract_path_text(_airbyte_data, 'patient_id_cp') as int) as patient_id_cp,
        cast(jsonb_extract_path_text(_airbyte_data, 'drug_generic') as varchar(255)) as drug_generic,
        cast(jsonb_extract_path_text(_airbyte_data, 'drug_brand') as varchar(255)) as drug_brand,
        cast(jsonb_extract_path_text(_airbyte_data, 'drug_name') as varchar(255)) as drug_name,
        cast(jsonb_extract_path_text(_airbyte_data, 'rx_message_key') as varchar(80)) as rx_message_key,
        cast(jsonb_extract_path_text(_airbyte_data, 'rx_message_text') as varchar(255)) as rx_message_text,
        cast(jsonb_extract_path_text(_airbyte_data, 'rx_gsn') as int) as rx_gsn,
        cast(jsonb_extract_path_text(_airbyte_data, 'drug_gsns') as varchar(255)) as drug_gsns,
        cast(jsonb_extract_path_text(_airbyte_data, 'refills_left') as decimal(5, 2)) as refills_left,
        cast(jsonb_extract_path_text(_airbyte_data, 'refills_original') as decimal(5, 2)) as refills_original,
        cast(jsonb_extract_path_text(_airbyte_data, 'qty_left') as decimal(10, 3)) as qty_left,
        cast(jsonb_extract_path_text(_airbyte_data, 'qty_original') as decimal(10, 3)) as qty_original,
        cast(jsonb_extract_path_text(_airbyte_data, 'sig_actual') as varchar(255)) as sig_actual,
        cast(jsonb_extract_path_text(_airbyte_data, 'sig_initial') as varchar(255)) as sig_initial,
        cast(jsonb_extract_path_text(_airbyte_data, 'sig_clean') as varchar(255)) as sig_clean,
        cast(jsonb_extract_path_text(_airbyte_data, 'sig_qty') as decimal(10, 3)) as sig_qty,
        cast(jsonb_extract_path_text(_airbyte_data, 'sig_v1_qty') as decimal(10, 3)) as sig_v1_qty,
        cast(jsonb_extract_path_text(_airbyte_data, 'sig_v1_days') as int) as sig_v1_days,
        cast(jsonb_extract_path_text(_airbyte_data, 'sig_v1_qty_per_day') as decimal(10, 2)) as sig_v1_qty_per_day,
        cast(jsonb_extract_path_text(_airbyte_data, 'sig_days') as int) as sig_days,
        cast(
            jsonb_extract_path_text(_airbyte_data, 'sig_qty_per_day_default') as decimal(6, 3)
        ) as sig_qty_per_day_default,
        cast(
            jsonb_extract_path_text(_airbyte_data, 'sig_qty_per_day_actual') as decimal(6, 3)
        ) as sig_qty_per_day_actual,
        cast(jsonb_extract_path_text(_airbyte_data, 'sig_durations') as varchar(255)) as sig_durations,
        cast(jsonb_extract_path_text(_airbyte_data, 'sig_qtys_per_time') as varchar(255)) as sig_qtys_per_time,
        cast(jsonb_extract_path_text(_airbyte_data, 'sig_frequencies') as varchar(255)) as sig_frequencies,
        cast(
            jsonb_extract_path_text(_airbyte_data, 'sig_frequency_numerators') as varchar(255)
        ) as sig_frequency_numerators,
        cast(
            jsonb_extract_path_text(_airbyte_data, 'sig_frequency_denominators') as varchar(255)
        ) as sig_frequency_denominators,
        cast(jsonb_extract_path_text(_airbyte_data, 'sig_v2_qty') as decimal(10, 3)) as sig_v2_qty,
        cast(jsonb_extract_path_text(_airbyte_data, 'sig_v2_days') as int) as sig_v2_days,
        cast(jsonb_extract_path_text(_airbyte_data, 'sig_v2_qty_per_day') as decimal(10, 3)) as sig_v2_qty_per_day,
        cast(jsonb_extract_path_text(_airbyte_data, 'sig_v2_unit') as varchar(255)) as sig_v2_unit,
        cast(jsonb_extract_path_text(_airbyte_data, 'sig_v2_conf_score') as decimal(10, 3)) as sig_v2_conf_score,
        cast(jsonb_extract_path_text(_airbyte_data, 'sig_v2_dosages') as varchar(255)) as sig_v2_dosages,
        cast(jsonb_extract_path_text(_airbyte_data, 'sig_v2_scores') as varchar(255)) as sig_v2_scores,
        cast(jsonb_extract_path_text(_airbyte_data, 'sig_v2_frequencies') as varchar(255)) as sig_v2_frequencies,
        cast(jsonb_extract_path_text(_airbyte_data, 'sig_v2_durations') as varchar(255)) as sig_v2_durations,
        cast(jsonb_extract_path_text(_airbyte_data, 'rx_autofill') as int) as rx_autofill,
        cast(jsonb_extract_path_text(_airbyte_data, 'refill_date_first') as timestamp) as refill_date_first,
        cast(jsonb_extract_path_text(_airbyte_data, 'refill_date_last') as timestamp) as refill_date_last,
        cast(jsonb_extract_path_text(_airbyte_data, 'refill_date_manual') as timestamp) as refill_date_manual,
        cast(jsonb_extract_path_text(_airbyte_data, 'refill_date_default') as timestamp) as refill_date_default,
        cast(jsonb_extract_path_text(_airbyte_data, 'rx_status') as int) as rx_status,
        cast(jsonb_extract_path_text(_airbyte_data, 'rx_stage') as varchar(80)) as rx_stage,
        cast(jsonb_extract_path_text(_airbyte_data, 'rx_source') as varchar(80)) as rx_source,
        cast(jsonb_extract_path_text(_airbyte_data, 'rx_transfer') as varchar(80)) as rx_transfer,
        cast(jsonb_extract_path_text(_airbyte_data, 'rx_date_transferred') as timestamp) as rx_date_transferred,
        cast(jsonb_extract_path_text(_airbyte_data, 'provider_npi') as varchar(80)) as provider_npi,
        cast(jsonb_extract_path_text(_airbyte_data, 'provider_first_name') as varchar(80)) as provider_first_name,
        cast(jsonb_extract_path_text(_airbyte_data, 'provider_last_name') as varchar(80)) as provider_last_name,
        cast(jsonb_extract_path_text(_airbyte_data, 'provider_clinic') as varchar(80)) as clinic_name,
        cast(jsonb_extract_path_text(_airbyte_data, 'provider_phone') as varchar(10)) as provider_phone,
        cast(jsonb_extract_path_text(_airbyte_data, 'rx_date_changed') as timestamp) as rx_date_changed,
        cast(jsonb_extract_path_text(_airbyte_data, 'rx_date_expired') as timestamp) as rx_date_expired,
        cast(jsonb_extract_path_text(_airbyte_data, 'rx_date_added') as timestamp) as rx_date_added,
        cast(jsonb_extract_path_text(_airbyte_data, 'created_at') as timestamp) as created_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'updated_at') as timestamp) as updated_at
    from
        "datawarehouse"."raw"._airbyte_raw_goodpill_gp_rxs_single
)

select
    *
from rxs_single

    where updated_at > (select max(updated_at) from "datawarehouse".dev_analytics."rxs_single")