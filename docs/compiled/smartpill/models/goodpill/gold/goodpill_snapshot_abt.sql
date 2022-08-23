with goodpill_snapshot as (
    with psh as (
        -- join with dimension patient right away, to join later with clinics
        select distinct on (patient_id_cp)
            pat.patient_id_cp,
            pme.event_date as dw_patient_event_date,
            pme.event_name as dw_patient_status,
            max(
                case
                    when pme.event_name = 'PATIENT_ACTIVE' then pme.event_date
                end
            ) over(partition by patient_id_cp) as patient_date_active,
            max(
                case
                    when pme.event_name = 'PATIENT_NO_RX' then pme.event_date
                end
            ) over(partition by patient_id_cp) as patient_date_no_rx,
            max(
                case
                    when pme.event_name = 'PATIENT_UNREGISTERED' then pme.event_date
                end
            ) over(partition by patient_id_cp) as patient_date_unregistered,
            max(
                case
                    when pme.event_name = 'PATIENT_DECEASED' then pme.event_date
                end
            ) over(partition by patient_id_cp) as patient_date_deceased,
            max(
                case
                    when pme.event_name = 'PATIENT_CHURNED_NO_FILLABLE_RX' then pme.event_date
                end
            ) over(partition by patient_id_cp) as patient_date_churned_no_fillable_rx,
            max(
                case
                    when pme.event_name = 'PATIENT_INACTIVE' then pme.event_date
                end
            ) over(partition by patient_id_cp) as patient_date_inactive,
            max(
                case
                    when pme.event_name = 'PATIENT_CHURNED_OTHER' then pme.event_date
                end
            ) over(partition by patient_id_cp) as patient_date_churned_other,
            clinics.clinic_name_cp as patient_clinic_name_coupon
        from "datawarehouse".dev_analytics."patients" as pat
        left join "datawarehouse".dev_analytics."clinics" as clinics on pat.clinic_id_coupon = clinics.clinic_id
        left join "datawarehouse".dev_analytics."patients_status_historic" as pme using (patient_id_cp)
        order by patient_id_cp, pme.event_date desc
    ),

    rh as (
        select
            patient_id_cp,
            rx_number,
            first_value(rx_numbers) over (
                partition by rx_number
                order by updated_at desc nulls last
            ) as rx_numbers,
            best_rx_number as rx_best_rx_number,
            first_value(provider_npi) over (
                partition by rx_number
                order by case when provider_npi is not null then 0 else 1 end, updated_at desc nulls last
            ) as rx_provider_npi,
            first_value(drug_generic) over (
                partition by rx_number
                order by case when drug_generic is not null then 0 else 1 end, updated_at desc nulls last
            ) as rx_drug_generic,
            drug_brand as rx_drug_brand,
            drug_name as rx_drug_name,
            rx_message_key as rx_message_key,
            rx_message_text as rx_message_text,
            rx_gsn as rx_gsn,
            drug_gsns as rx_drug_gsns,
            max_gsn as rx_max_gsn,
            refills_left as rx_refills_left,
            refills_original as rx_refills_original,
            refills_total as rx_refills_total,
            qty_left as rx_qty_left,
            qty_original as rx_qty_original,
            sig_actual as rx_sig_actual,
            sig_initial as rx_sig_initial,
            sig_clean as rx_sig_clean,
            sig_qty as rx_sig_qty,
            sig_v1_qty as rx_sig_v1_qty,
            sig_v1_days as rx_sig_v1_days,
            sig_v1_qty_per_day as rx_sig_v1_qty_per_day,
            sig_days as rx_sig_days,
            sig_qty_per_day as rx_sig_qty_per_day,
            sig_qty_per_day_default as rx_sig_qty_per_day_default,
            sig_qty_per_day_actual as rx_sig_qty_per_day_actual,
            sig_durations as rx_sig_durations,
            sig_qtys_per_time as rx_sig_qtys_per_time,
            sig_frequencies as rx_sig_frequencies,
            sig_frequency_numerators as rx_sig_frequency_numerators,
            sig_frequency_denominators as rx_sig_frequency_denominators,
            sig_v2_qty as rx_sig_v2_qty,
            sig_v2_days as rx_sig_v2_days,
            sig_v2_qty_per_day as rx_sig_v2_qty_per_day,
            sig_v2_unit as rx_sig_v2_unit,
            sig_v2_conf_score as rx_sig_v2_conf_score,
            sig_v2_dosages as rx_sig_v2_dosages,
            sig_v2_scores as rx_sig_v2_scores,
            sig_v2_frequencies as rx_sig_v2_frequencies,
            sig_v2_durations as rx_sig_v2_durations,
            rx_autofill as rx_autofill,
            refill_date_first as rx_refill_date_first,
            refill_date_last as rx_refill_date_last,
            refill_date_manual as rx_refill_date_manual,
            refill_date_next as rx_refill_date_next,
            rx_status as rx_status,
            rx_stage as rx_stage,
            rx_source as rx_source,
            rx_transfer as rx_transfer,
            rx_date_transferred as rx_date_transferred,
            provider_first_name as provider_first_name,
            provider_last_name as provider_last_name,
            provider_phone as provider_phone,
            clinic_name as rx_clinic_name,
            rx_date_changed as rx_date_changed,
            rx_date_expired as rx_date_expired,
            rx_date_added as rx_date_added,
            created_at as rx_created_at,
            updated_at as rx_updated_at,
            group_created_at as group_created_at,
            rx_clinic_name_cp
        from "datawarehouse".dev_analytics."rxs_joined"
    ),

    oi as (
        select
            patient_id_cp,
            rx_number,
            invoice_number as order_invoice_number,
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
            item_date_changed as item_date_changed,
            updated_at as item_date_updated,
            refill_date_last as item_refill_date_last,
            refill_date_manual as item_refill_date_manual,
            refill_date_default as item_refill_date_default,
            refill_target_date as item_refill_target_date,
            refill_target_days as item_refill_target_days,
            refill_target_rxs as item_refill_target_rxs,
            add_user_id as item_add_user_id,
            chg_user_id as item_chg_user_id,
            count_lines as item_count_lines,
            repacked_by as item_repacked_by,
            repacked_at as item_repacked_at
        from "datawarehouse".dev_analytics."order_items"
    ),

    o as (
        select
            patient_id_cp,
            patient_id_wc,
            invoice_number as order_invoice_number,
            order_date_added as order_date_added,
            order_date_dispensed as order_date_dispensed,
            order_date_shipped as order_date_shipped,
            order_date_returned as order_date_returned,
            count_items as order_count_items,
            count_filled as order_count_filled,
            count_nofill as order_count_nofill,
            order_source as order_source,
            order_stage_cp as order_stage_cp,
            order_stage_wc as order_stage_wc,
            order_status as order_status,
            order_address1,
            order_address2,
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
            order_date_delivered,
            order_date_expedited,
            order_date_expected,
            order_date_expected_initial,
            order_date_failed,
            add_user_id as order_add_user_id,
            chg_user_id as order_chg_user_id,
            shipping_speed as order_shipping_speed,
            order_note as order_note,
            rph_check as order_rph_check,
            tech_fill as order_tech_fill,
            order_city as order_city,
            order_state as order_state,
            order_zip as order_zip,
            updated_at as order_date_updated
        from "datawarehouse".dev_analytics."orders"
    )

    select distinct on (patient_id_cp, rx_number, order_invoice_number)
        *,
        coalesce(
            psh.patient_clinic_name_coupon,
            rh.rx_clinic_name_cp
        ) as clinic_coalesced_name,
        greatest(rh.group_created_at, oi.item_date_updated, o.order_date_updated) as dw_updated_at
    from psh
    left join rh using (patient_id_cp)
    left join oi using (rx_number, patient_id_cp)
    left join o using (order_invoice_number, patient_id_cp)
    order by
        patient_id_cp,
        rx_number,
        order_invoice_number,
        -- prioritize rxs which were updated before the order was dispensed
        rh.group_created_at <= o.order_date_dispensed desc,
        rh.group_created_at desc
)

select
    gds.order_invoice_number,
    gds.patient_id_cp,
    gds.rx_number,
    gds.dw_patient_event_date,
    gds.dw_patient_status,
    gds.patient_date_active,
    gds.patient_date_no_rx,
    gds.patient_date_unregistered,
    gds.patient_date_deceased,
    gds.patient_date_churned_no_fillable_rx,
    gds.patient_date_inactive,
    gds.patient_date_churned_other,
    gds.patient_clinic_name_coupon,
    gds.rx_numbers,
    gds.rx_best_rx_number,
    gds.rx_provider_npi,
    gds.rx_drug_generic,
    gds.rx_drug_brand,
    gds.rx_drug_name,
    gds.rx_message_key,
    gds.rx_message_text,
    gds.rx_gsn,
    gds.rx_drug_gsns,
    gds.rx_max_gsn,
    gds.rx_refills_left,
    gds.rx_refills_original,
    gds.rx_refills_total,
    gds.rx_qty_left,
    gds.rx_qty_original,
    gds.rx_sig_actual,
    gds.rx_sig_initial,
    gds.rx_sig_clean,
    gds.rx_sig_qty,
    gds.rx_sig_v1_qty,
    gds.rx_sig_v1_days,
    gds.rx_sig_v1_qty_per_day,
    gds.rx_sig_days,
    gds.rx_sig_qty_per_day,
    gds.rx_sig_qty_per_day_default,
    gds.rx_sig_qty_per_day_actual,
    gds.rx_sig_durations,
    gds.rx_sig_qtys_per_time,
    gds.rx_sig_frequencies,
    gds.rx_sig_frequency_numerators,
    gds.rx_sig_frequency_denominators,
    gds.rx_sig_v2_qty,
    gds.rx_sig_v2_days,
    gds.rx_sig_v2_qty_per_day,
    gds.rx_sig_v2_unit,
    gds.rx_sig_v2_conf_score,
    gds.rx_sig_v2_dosages,
    gds.rx_sig_v2_scores,
    gds.rx_sig_v2_frequencies,
    gds.rx_sig_v2_durations,
    gds.rx_autofill,
    gds.rx_refill_date_first,
    gds.rx_refill_date_last,
    gds.rx_refill_date_manual,
    gds.rx_refill_date_next,
    gds.rx_status,
    gds.rx_stage,
    gds.rx_source,
    gds.rx_transfer,
    gds.rx_date_transferred,
    gds.provider_first_name,
    gds.provider_last_name,
    gds.provider_phone,
    gds.rx_clinic_name,
    gds.rx_date_changed,
    gds.rx_date_expired,
    gds.rx_date_added,
    gds.rx_created_at,
    gds.rx_updated_at,
    gds.group_created_at,
    gds.rx_clinic_name_cp,
    gds.item_groups,
    gds.item_rx_dispensed_id,
    gds.item_stock_level_initial,
    gds.item_rx_message_keys_initial,
    gds.item_patient_autofill_initial,
    gds.item_rx_autofill_initial,
    gds.item_rx_numbers_initial,
    gds.item_zscore_initial,
    gds.item_refills_dispensed_default,
    gds.item_refills_dispensed_actual,
    gds.item_days_dispensed_default,
    gds.item_days_dispensed_actual,
    gds.item_qty_dispensed_default,
    gds.item_qty_dispensed_actual,
    gds.item_price_dispensed_default,
    gds.item_price_dispensed_actual,
    gds.item_qty_pended_total,
    gds.item_qty_pended_repacks,
    gds.item_count_pended_total,
    gds.item_count_pended_repacks,
    gds.item_message_keys,
    gds.item_message_text,
    gds.item_type,
    gds.item_added_by,
    gds.item_date_added,
    gds.item_date_changed,
    gds.item_date_updated,
    gds.item_refill_date_last,
    gds.item_refill_date_manual,
    gds.item_refill_date_default,
    gds.item_refill_target_date,
    gds.item_refill_target_days,
    gds.item_refill_target_rxs,
    gds.item_add_user_id,
    gds.item_chg_user_id,
    gds.item_count_lines,
    gds.item_repacked_by,
    gds.item_repacked_at,
    gds.patient_id_wc,
    gds.order_date_added,
    gds.order_date_dispensed,
    gds.order_date_shipped,
    gds.order_date_returned,
    gds.order_count_items,
    gds.order_count_filled,
    gds.order_count_nofill,
    gds.order_source,
    gds.order_stage_cp,
    gds.order_stage_wc,
    gds.order_status,
    gds.order_address1,
    gds.order_address2,
    gds.order_invoice_doc_id,
    gds.order_tracking_number,
    gds.order_payment_total_default,
    gds.order_payment_total_actual,
    gds.order_payment_fee_default,
    gds.order_payment_fee_actual,
    gds.order_payment_due_default,
    gds.order_payment_due_actual,
    gds.order_payment_date_autopay,
    gds.order_payment_method_actual,
    gds.order_payment_coupon,
    gds.order_date_delivered,
    gds.order_date_expedited,
    gds.order_date_expected,
    gds.order_date_expected_initial,
    gds.order_date_failed,
    gds.order_add_user_id,
    gds.order_chg_user_id,
    gds.order_shipping_speed,
    gds.order_note,
    gds.order_rph_check,
    gds.order_tech_fill,
    gds.order_city,
    gds.order_state,
    gds.order_zip,
    gds.order_date_updated,
    gds.dw_updated_at,
    clinics.clinic_id,
    clinics.clinic_name_cp,
    clinics.clinic_rx_date_added_first,
    clinics.clinic_rx_date_added_last,
    clinics.verified as clinic_verified,
    clinics.created_at as clinic_created_at,
    clinics.updated_at as clinic_updated_at,
    dw_clinics.clinic_id as dw_clinic_id,
    dw_clinics.clinic_name as dw_clinic_name,
    dw_clinics.clinic_name_cp as dw_clinic_name_cp,
    dw_clinics.clinic_address as dw_clinic_address,
    dw_clinics.clinic_street as dw_clinic_street,
    dw_clinics.clinic_city as dw_clinic_city,
    dw_clinics.clinic_state as dw_clinic_state,
    dw_clinics.clinic_zip as dw_clinic_zip,
    dw_clinics.clinic_phone as dw_clinic_phone,
    dw_clinics.clinic_id_sf as dw_clinic_id_sf,
    dw_clinics.created_at as dw_clinic_created_at,
    dw_clinics.updated_at as dw_clinic_updated_at,
    clinics_groups.id as clinic_groups_id,
    clinics_groups.meta_group as clinic_groups_meta_group,
    clinics_groups.meta_template as clinic_groups_meta_template,
    clinics_groups.commentary as clinic_groups_commentary,
    clinics_groups.created_at as clinic_groups_created_at,
    clinics_groups.updated_at as clinic_groups_updated_at,
    dw_clinics_groups.clinic_group_id as dw_clinic_group_id,
    dw_clinics_groups.clinic_group_name as dw_clinic_group_name,
    dw_clinics_groups.clinic_group_id_sf as dw_clinic_group_id_sf,
    dw_clinics_groups.clinic_group_domain as dw_clinic_group_domain,
    dw_clinics_groups.created_at as dw_clinic_groups_created_at,
    dw_clinics_groups.updated_at as dw_clinic_groups_updated_at,
    drugs.drug_brand,
    drugs.drug_gsns,
    drugs.price30 as drug_price30,
    drugs.price90 as drug_price90,
    drugs.price_retail as drug_price_retail,
    drugs.price_goodrx as drug_price_goodrx,
    drugs.price_nadac as drug_price_nadac,
    drugs.price_coalesced as drug_price_coalesced,
    drugs.qty_repack as drug_qty_repack,
    drugs.count_ndcs as drug_count_ndcs,
    providers.provider_id as provider_id,
    providers.verified as provider_verified,
    providers.first_rx_sent_date as provider_first_rx_sent_date,
    providers.last_rx_sent_date as provider_last_rx_sent_date,
    dw_providers.provider_name as dw_provider_name,
    dw_providers.provider_phone as dw_provider_phone,
    dw_providers.provider_id_sf as dw_provider_id_sf,
    patients.patient_date_registered as patient_date_registered,
    patients.patient_date_added as patient_date_added,
    patients.first_name as patient_first_name,
    patients.last_name as patient_last_name,
    patients.birth_date as patient_birth_date,
    patients.birth_date as patient_language,
    patients.phone1 as patient_phone1,
    patients.phone2 as patient_phone2,
    patients.patient_address as patient_address,
    patients.patient_city as patient_city,
    patients.patient_state as patient_state,
    patients.patient_zip as patient_zip,
    patients.payment_card_type as patient_payment_card_type,
    patients.payment_card_last4 as patient_payment_card_last4,
    patients.payment_card_date_expired as patient_payment_card_date_expired,
    patients.payment_card_autopay as patient_payment_card_autopay,
    patients.payment_method_default as patient_payment_method_default,
    patients.patient_date_first_rx_received,
    patients.patient_date_first_dispensed,
    patients.patient_date_first_expected_by,
    patients.refills_used as patient_refills_used,
    patients.email as patient_email,
    patients.patient_autofill,
    patients.initial_invoice_number as patient_initial_invoice_number,
    patients.patient_note,
    patients.allergies_none as patient_allergies_none,
    patients.allergies_cephalosporins as patient_allergies_cephalosporins,
    patients.allergies_sulfa as patient_allergies_sulfa,
    patients.allergies_aspirin as patient_allergies_aspirin,
    patients.allergies_penicillin as patient_allergies_penicillin,
    patients.allergies_erythromycin as patient_allergies_erythromycin,
    patients.allergies_codeine as patient_allergies_codeine,
    patients.allergies_nsaids as patient_allergies_nsaids,
    patients.allergies_salicylates as patient_allergies_salicylates,
    patients.allergies_azithromycin as patient_allergies_azithromycin,
    patients.allergies_amoxicillin as patient_allergies_amoxicillin,
    patients.allergies_tetracycline as patient_allergies_tetracycline,
    patients.allergies_other as patient_allergies_other,
    patients.medications_other as patient_medications_other,
    patients.pharmacy_npi as pharmacy_npi,
    patients.pharmacy_name as pharmacy_name,
    patients.pharmacy_phone as pharmacy_phone,
    patients.pharmacy_fax as pharmacy_fax,
    patients.pharmacy_address as pharmacy_address,
    patients.payment_coupon as patient_payment_coupon,
    patients.tracking_coupon as patient_tracking_coupon
from goodpill_snapshot as gds
left join "datawarehouse".dev_analytics."drugs" as drugs on drugs.generic_name = gds.rx_drug_generic
left join "datawarehouse".dev_analytics."patients" as patients on patients.patient_id_cp = gds.patient_id_cp
left join "datawarehouse".dev_analytics."providers" as providers on providers.npi = gds.rx_provider_npi
left join "datawarehouse".dev_analytics."dw_providers" as dw_providers on dw_providers.provider_npi = providers.npi
left join "datawarehouse".dev_analytics."clinics" as clinics on clinics.clinic_name_cp = gds.clinic_coalesced_name
left join "datawarehouse".dev_analytics."dw_clinics" as dw_clinics on dw_clinics.clinic_id = clinics.clinic_id
left join "datawarehouse".dev_analytics."dw_clinics_groups" as dw_clinics_groups on
        dw_clinics_groups.clinic_group_id = dw_clinics.clinic_group_id
left join "datawarehouse".dev_analytics."clinics_groups" as clinics_groups on
        clinics_groups.id = dw_clinics.clinic_group_id