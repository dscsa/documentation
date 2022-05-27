with rxs_grouped_unnested as (
    select
        *
    from "datawarehouse".dev_analytics."rxs_grouped", unnest(string_to_array(trim(both ',' from rx_numbers), ',')) as rx_number
)

select distinct on (rs.rx_number, rs.updated_at)
    rs.rx_number as rx_number,
    rg.rx_numbers as rx_numbers,
    rg.best_rx_number as best_rx_number,
    rs.patient_id_cp as patient_id_cp,
    coalesce(rg.drug_generic, rs.drug_generic) as drug_generic,
    coalesce(rg.drug_brand, rs.drug_brand) as drug_brand,
    coalesce(rg.drug_name, rs.drug_name) as drug_name,
    rs.rx_message_key as rx_message_key,
    rs.rx_message_text as rx_message_text,
    rs.rx_gsn as rx_gsn,
    coalesce(rg.drug_gsns, rs.drug_gsns) as drug_gsns,
    rg.max_gsn as max_gsn,
    rs.refills_left as refills_left,
    rs.refills_original as refills_original,
    rg.refills_total as refills_total,
    rs.qty_left as qty_left,
    rs.qty_original as qty_original,
    rs.sig_actual as sig_actual,
    rs.sig_initial as sig_initial,
    rs.sig_clean as sig_clean,
    rs.sig_qty as sig_qty,
    rs.sig_v1_qty as sig_v1_qty,
    rs.sig_v1_days as sig_v1_days,
    rs.sig_v1_qty_per_day as sig_v1_qty_per_day,
    rs.sig_days as sig_days,
    rg.sig_qty_per_day as sig_qty_per_day,
    rs.sig_qty_per_day_default as sig_qty_per_day_default,
    rs.sig_qty_per_day_actual as sig_qty_per_day_actual,
    rs.sig_durations as sig_durations,
    rs.sig_qtys_per_time as sig_qtys_per_time,
    rs.sig_frequencies as sig_frequencies,
    rs.sig_frequency_numerators as sig_frequency_numerators,
    rs.sig_frequency_denominators as sig_frequency_denominators,
    rs.sig_v2_qty as sig_v2_qty,
    rs.sig_v2_days as sig_v2_days,
    rs.sig_v2_qty_per_day as sig_v2_qty_per_day,
    rs.sig_v2_unit as sig_v2_unit,
    rs.sig_v2_conf_score as sig_v2_conf_score,
    rs.sig_v2_dosages as sig_v2_dosages,
    rs.sig_v2_scores as sig_v2_scores,
    rs.sig_v2_frequencies as sig_v2_frequencies,
    rs.sig_v2_durations as sig_v2_durations,
    coalesce(rg.rx_autofill, rs.rx_autofill) as rx_autofill,
    coalesce(rg.refill_date_first, rs.refill_date_first) as refill_date_first,
    coalesce(rg.refill_date_last, rs.refill_date_last) as refill_date_last,
    coalesce(rg.refill_date_manual, rs.refill_date_manual) as refill_date_manual,
    coalesce(rg.refill_date_default, rs.refill_date_default) as refill_date_default,
    rg.refill_date_next as refill_date_next,
    rs.rx_status as rx_status,
    rs.rx_stage as rx_stage,
    rs.rx_source as rx_source,
    rs.rx_transfer as rx_transfer,
    rs.rx_date_transferred as rx_date_transferred,
    rs.provider_npi as provider_npi,
    rs.provider_first_name as provider_first_name,
    rs.provider_last_name as provider_last_name,
    rs.clinic_name as clinic_name,
    rs.provider_phone as provider_phone,
    rs.rx_date_changed as rx_date_changed,
    rs.rx_date_expired as rx_date_expired,
    rs.rx_date_added as rx_date_added,
    rs.created_at as created_at,
    rs.updated_at as updated_at,
    rg.created_at as group_created_at,
    c_normal_names.clinic_normalized_name as clinic_normalized_name,
    c_npi.clinic_name as clinic_name_provider_npi,
    c_provider_names.clinic_name as clinic_name_provider_name
from "datawarehouse".dev_analytics."rxs_single" as rs
left join rxs_grouped_unnested as rg on (rs.rx_number = cast(rg.rx_number as int))
left join "datawarehouse".dev_analytics."clinics_meta_normalized_names" as c_normal_names on c_normal_names.clinic_name = rs.clinic_name
left join "datawarehouse".dev_analytics."clinics_meta_npi_numbers" as c_npi on c_npi.npi_number = rs.provider_npi
left join
    "datawarehouse".dev_analytics."clinics_meta_provider_names" as c_provider_names on
        c_provider_names.provider_name = concat(trim(rs.provider_first_name), ' ', trim(rs.provider_last_name))

    where rs.updated_at > (select max(updated_at) from "datawarehouse".dev_analytics."rxs_joined")

order by
    rs.rx_number,
    rs.updated_at,
    -- prioritize the rxs_single that was updated before the group was created
    rs.updated_at <= rg.created_at + interval '1' day desc,
    rg.created_at desc,
    c_npi.updated_at desc,
    c_provider_names.updated_at desc