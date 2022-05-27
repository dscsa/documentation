with rxs_grouped as (
    select
        _airbyte_emitted_at,
        _airbyte_ab_id,
        cast(jsonb_extract_path_text(_airbyte_data, 'patient_id_cp') as int) as patient_id_cp,
        cast(jsonb_extract_path_text(_airbyte_data, 'drug_generic') as varchar(255)) as drug_generic,
        cast(jsonb_extract_path_text(_airbyte_data, 'drug_brand') as varchar(255)) as drug_brand,
        cast(jsonb_extract_path_text(_airbyte_data, 'drug_name') as varchar(255)) as drug_name,
        cast(jsonb_extract_path_text(_airbyte_data, 'sig_qty_per_day') as decimal(6, 3)) as sig_qty_per_day,
        cast(jsonb_extract_path_text(_airbyte_data, 'rx_message_keys') as varchar(255)) as rx_message_keys,
        cast(jsonb_extract_path_text(_airbyte_data, 'max_gsn') as int) as max_gsn,
        cast(jsonb_extract_path_text(_airbyte_data, 'drug_gsns') as varchar(255)) as drug_gsns,
        cast(jsonb_extract_path_text(_airbyte_data, 'refills_total') as decimal(5, 2)) as refills_total,
        cast(jsonb_extract_path_text(_airbyte_data, 'qty_total') as decimal(11, 3)) as qty_total,
        cast(jsonb_extract_path_text(_airbyte_data, 'rx_autofill') as int) as rx_autofill,
        cast(jsonb_extract_path_text(_airbyte_data, 'refill_date_first') as timestamp) as refill_date_first,
        cast(jsonb_extract_path_text(_airbyte_data, 'refill_date_last') as timestamp) as refill_date_last,
        cast(jsonb_extract_path_text(_airbyte_data, 'refill_date_next') as timestamp) as refill_date_next,
        cast(jsonb_extract_path_text(_airbyte_data, 'refill_date_manual') as timestamp) as refill_date_manual,
        cast(jsonb_extract_path_text(_airbyte_data, 'refill_date_default') as timestamp) as refill_date_default,
        cast(jsonb_extract_path_text(_airbyte_data, 'best_rx_number') as int) as best_rx_number,
        cast(jsonb_extract_path_text(_airbyte_data, 'rx_numbers') as varchar(255)) as rx_numbers,
        cast(jsonb_extract_path_text(_airbyte_data, 'rx_sources') as varchar(80)) as rx_sources,
        cast(jsonb_extract_path_text(_airbyte_data, 'rx_date_changed') as timestamp) as rx_date_changed,
        cast(jsonb_extract_path_text(_airbyte_data, 'rx_date_expired') as timestamp) as rx_date_expired,
        cast(jsonb_extract_path_text(_airbyte_data, 'rx_date_transferred') as timestamp) as rx_date_transferred,
        cast(jsonb_extract_path_text(_airbyte_data, 'created_at') as timestamp) as created_at
    from
        "datawarehouse"."raw"._airbyte_raw_goodpill_gp_rxs_grouped
)

select
    *
from rxs_grouped

    where created_at > (select max(created_at) from "datawarehouse".dev_analytics."rxs_grouped")
