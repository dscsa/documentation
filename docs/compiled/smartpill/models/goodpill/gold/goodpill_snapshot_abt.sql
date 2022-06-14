with goodpill_snapshot as (
    with psh as (
        -- join with dimension patient right away, to join later with clinics
        select
            patient_id_cp,
            event_date as patient_event_date,
            event_name as patient_status,
            max(
                case
                    when event_name = 'PATIENT_ACTIVE' then event_date
                end
            ) over(partition by patient_id_cp) as date_patient_active,
            max(
                case
                    when event_name = 'PATIENT_NO_RX' then event_date
                end
            ) over(partition by patient_id_cp) as date_patient_no_rx,
            max(
                case
                    when event_name = 'PATIENT_UNREGISTERED' then event_date
                end
            ) over(partition by patient_id_cp) as date_patient_unregistered,
            max(
                case
                    when event_name = 'PATIENT_DECEASED' then event_date
                end
            ) over(partition by patient_id_cp) as date_patient_deceased,
            max(
                case
                    when event_name = 'PATIENT_CHURNED_NO_FILLABLE_RX' then event_date
                end
            ) over(partition by patient_id_cp) as date_patient_churned_no_fillable_rx,
            max(
                case
                    when event_name = 'PATIENT_INACTIVE' then event_date
                end
            ) over(partition by patient_id_cp) as date_patient_inactive,
            max(
                case
                    when event_name = 'PATIENT_CHURNED_OTHER' then event_date
                end
            ) over(partition by patient_id_cp) as date_patient_churned_other,
            p.clinic_name_coupon as patient_clinic_name_coupon
        from "datawarehouse".dev_analytics."patients" as p
        left join "datawarehouse".dev_analytics."patients_status_historic" as pme using (patient_id_cp)
        order by patient_id_cp, patient_event_date desc
    ),

    rh as (
        select
            rh.patient_id_cp,
            rh.rx_number,
            first_value(rx_numbers) over (
                partition by rx_number
                order by updated_at desc nulls last
            ) as rx_numbers,
            rh.best_rx_number,
            first_value(provider_npi) over (
                partition by rx_number
                order by case when provider_npi is not null then 0 else 1 end, updated_at desc nulls last
            ) as rx_provider_npi,
            first_value(drug_generic) over (
                partition by rx_number
                order by case when drug_generic is not null then 0 else 1 end, updated_at desc nulls last
            ) as rx_drug_generic,
            rh.drug_brand as rx_drug_brand,
            rh.drug_name as rx_drug_name,
            rh.rx_message_key as rx_message_key,
            rh.rx_message_text as rx_message_text,
            rh.rx_gsn as rx_gsn,
            rh.drug_gsns as rx_drug_gsns,
            rh.max_gsn as rx_max_gsn,
            rh.refills_left as rx_refills_left,
            rh.refills_original as rx_refills_original,
            rh.refills_total as rx_refills_total,
            rh.qty_left as rx_qty_left,
            rh.qty_original as rx_qty_original,
            rh.sig_actual as rx_sig_actual,
            rh.sig_initial as rx_sig_initial,
            rh.sig_clean as rx_sig_clean,
            rh.sig_qty as rx_sig_qty,
            rh.sig_v1_qty as rx_sig_v1_qty,
            rh.sig_v1_days as rx_sig_v1_days,
            rh.sig_v1_qty_per_day as rx_sig_v1_qty_per_day,
            rh.sig_days as rx_sig_days,
            rh.sig_qty_per_day as rx_sig_qty_per_day,
            rh.sig_qty_per_day_default as rx_sig_qty_per_day_default,
            rh.sig_qty_per_day_actual as rx_sig_qty_per_day_actual,
            rh.sig_durations as rx_sig_durations,
            rh.sig_qtys_per_time as rx_sig_qtys_per_time,
            rh.sig_frequencies as rx_sig_frequencies,
            rh.sig_frequency_numerators as rx_sig_frequency_numerators,
            rh.sig_frequency_denominators as rx_sig_frequency_denominators,
            rh.sig_v2_qty as rx_sig_v2_qty,
            rh.sig_v2_days as rx_sig_v2_days,
            rh.sig_v2_qty_per_day as rx_sig_v2_qty_per_day,
            rh.sig_v2_unit as rx_sig_v2_unit,
            rh.sig_v2_conf_score as rx_sig_v2_conf_score,
            rh.sig_v2_dosages as rx_sig_v2_dosages,
            rh.sig_v2_scores as rx_sig_v2_scores,
            rh.sig_v2_frequencies as rx_sig_v2_frequencies,
            rh.sig_v2_durations as rx_sig_v2_durations,
            rh.rx_autofill as rx_autofill,
            rh.refill_date_first as rx_refill_date_first,
            rh.refill_date_last as rx_refill_date_last,
            rh.refill_date_manual as rx_refill_date_manual,
            rh.refill_date_next as rx_refill_date_next,
            rh.rx_status as rx_status,
            rh.rx_stage as rx_stage,
            rh.rx_source as rx_source,
            rh.rx_transfer as rx_transfer,
            rh.rx_date_transferred as rx_date_transferred,
            rh.provider_first_name as provider_first_name,
            rh.provider_last_name as provider_last_name,
            rh.provider_phone as provider_phone,
            rh.clinic_name as rx_clinic_name,
            rh.rx_date_changed as rx_date_changed,
            rh.rx_date_expired as rx_date_expired,
            rh.rx_date_added as rx_date_added,
            rh.created_at as rx_created_at,
            rh.updated_at as rx_updated_at,
            rh.group_created_at as group_created_at,
            rh.clinic_normalized_name as rx_clinic_normalized_name,
            rh.clinic_name_provider_npi as rx_clinic_name_provider_npi,
            rh.clinic_name_provider_name as rx_clinic_name_provider_name
        from "datawarehouse".dev_analytics."rxs_joined" as rh
    ),

    oi as (
        select
            patient_id_cp,
            rx_number,
            invoice_number,
            groups as item_groups,
            rx_dispensed_id as item_rx_dispensed_id,
            stock_level_initial as item_stock_level_initial,
            rx_message_keys_initial as item_rx_message_keys_initial,
            patient_autofill_initial as item_patient_autofill_initial,
            rx_autofill_initial as item_rx_autofill_initial,
            rx_numbers_initial as item_rx_numbers_initial,
            zscore_initial as item_zscore_initial,
            refills_dispensed_default as item_refills_dispensed_default,
            refills_dispensed_actual as item_refills_dispensed_actual,
            days_dispensed_default as item_days_dispensed_default,
            days_dispensed_actual as item_days_dispensed_actual,
            qty_dispensed_default as item_qty_dispensed_default,
            qty_dispensed_actual as item_qty_dispensed_actual,
            price_dispensed_default as item_price_dispensed_default,
            price_dispensed_actual as item_price_dispensed_actual,
            qty_pended_total as item_qty_pended_total,
            qty_pended_repacks as item_qty_pended_repacks,
            count_pended_total as item_count_pended_total,
            count_pended_repacks as item_count_pended_repacks,
            item_message_keys as item_message_keys,
            item_message_text as item_message_text,
            item_type as item_type,
            item_added_by as item_added_by,
            item_date_added as item_date_added,
            updated_at as item_date_updated,
            updated_at as date_item_updated,
            refill_date_last as item_refill_date_last,
            refill_date_manual as item_refill_date_manual,
            refill_date_default as item_refill_date_default,
            refill_target_date as item_refill_target_date,
            refill_target_days as item_refill_target_days,
            refill_target_rxs as item_refill_target_rxs
        from "datawarehouse".dev_analytics."order_items" as oi
    ),

    o as (
        select
            patient_id_cp,
            invoice_number,
            order_date_added as date_order_added,
            order_date_dispensed as date_order_dispensed,
            order_date_shipped as date_order_shipped,
            order_date_returned as date_order_returned,
            count_items as order_count_items,
            count_filled as order_count_filled,
            count_nofill as order_count_nofill,
            order_source as order_source,
            order_stage_cp as order_stage_cp,
            order_status as order_status,
            invoice_doc_id as order_invoice_doc_id,
            tracking_number as order_tracking_number,
            payment_total_default as order_payment_total_default,
            payment_total_actual as order_payment_total_actual,
            payment_fee_default as order_payment_fee_default,
            payment_fee_actual as order_payment_fee_actual,
            payment_due_default as order_payment_due_default,
            payment_due_actual as order_payment_due_actual,
            payment_date_autopay as order_payment_date_autopay,
            payment_method_actual as order_payment_method_actual,
            order_payment_coupon as order_payment_coupon,
            order_note as order_note,
            rph_check as order_rph_check,
            tech_fill as order_tech_fill,
            order_city as order_city,
            order_state as order_state,
            order_zip as order_zip,
            updated_at as order_date_updated
        from "datawarehouse".dev_analytics."orders"
    )

    select distinct on (patient_id_cp, rx_number, invoice_number)
        *,
        coalesce(
            patient_clinic_name_coupon,
            rx_clinic_name_provider_npi,
            rx_clinic_normalized_name,
            rx_clinic_name_provider_name,
            rh.rx_clinic_name
        ) as clinic_coalesced_name,
        greatest(rh.group_created_at, item_date_updated, order_date_updated) as updated_at
    from psh
    left join rh using (patient_id_cp)
    left join oi using (rx_number, patient_id_cp)
    left join o using (invoice_number, patient_id_cp)
    order by
        patient_id_cp,
        rx_number,
        invoice_number,
        -- prioritize rxs which were updated before the order was dispensed
        rh.group_created_at <= o.date_order_dispensed desc,
        patient_event_date desc,
        rh.group_created_at desc
)

select
    gs.*,
    clinics_groups.meta_group as clinic_meta_group,
    clinics_groups.meta_template as clinic_meta_template,
    drugs.brand_name as drug_brand,
    drugs.price30 as drug_price30,
    drugs.price90 as drug_price90,
    drugs.price_retail as drug_price_retail,
    drugs.price_goodrx as drug_price_goodrx,
    drugs.price_nadac as drug_price_nadac,
    drugs.price_coalesced as drug_price_coalesced,
    drugs.qty_repack as drug_qty_repack,
    drugs.count_ndcs as drug_count_ndcs,
    gs.order_city as location_city,
    gs.order_state as location_state,
    gs.order_zip as location_zip_code,
    providers.provider_first_rx_sent_date as provider_first_rx_sent_date,
    providers.provider_last_rx_sent_date as provider_last_rx_sent_date,
    patients.patient_date_registered as patient_date_registered,
    patients.patient_date_added as patient_date_added,
    patients.first_name as patient_first_name,
    patients.last_name as patient_last_name,
    patients.birth_date as patient_birth_date,
    patients.phone1 as patient_phone1,
    patients.phone2 as patient_phone2,
    patients.patient_address as patient_address,
    patients.patient_city as patient_city,
    patients.patient_state as patient_state,
    patients.patient_zip as patient_zip,
    patients.payment_card_type as patient_payment_card_type,
    patients.payment_card_last4 as patient_payment_card_last4,
    patients.payment_card_date_expired as patient_payment_card_date_expired,
    patients.payment_method_default as patient_payment_method_default,
    patients.refills_used as patient_refills_used,
    patients.date_first_rx_received as patient_date_first_rx_received,
    patients.date_first_dispensed as patient_date_first_dispensed,
    patients.date_first_expected_by as patient_date_first_expected_by,
    patients.pharmacy_npi as pharmacy_npi,
    patients.pharmacy_name as pharmacy_name,
    patients.pharmacy_phone as pharmacy_phone,
    patients.pharmacy_fax as pharmacy_fax,
    patients.pharmacy_address as pharmacy_address,
    patients.payment_coupon as patient_payment_coupon,
    patients.tracking_coupon as patient_tracking_coupon,
    md5(cast(gs.patient_id_cp || gs.rx_numbers || gs.invoice_number as varchar)) as unique_id
from goodpill_snapshot gs
left join "datawarehouse".dev_analytics."drugs" as drugs on gs.rx_drug_generic = drugs.generic_name
left join "datawarehouse".dev_analytics."patients" as patients on gs.patient_id_cp = patients.patient_id_cp
left join "datawarehouse".dev_analytics."providers" as providers on gs.rx_provider_npi = providers.provider_npi
left join "datawarehouse".dev_analytics."clinics_groups" as clinics_groups
          on gs.clinic_coalesced_name ilike concat('%', clinics_groups.meta_group, '%')