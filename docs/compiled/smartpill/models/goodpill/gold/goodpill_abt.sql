

with _patients as (
    select _patients.patient_id_cp, cmc.clinic_id
    from "datawarehouse".goodpill."patients" as _patients
    left join "datawarehouse".goodpill."clinic_coupons" as cmc on
        _patients.payment_coupon = cmc.coupon_code or _patients.tracking_coupon = cmc.coupon_code
),
goodpill_snapshot as (
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
            ) over(partition by patient_id_cp) as patient_date_churned_other
        from _patients as pat
        left join "datawarehouse".goodpill."clinics" as clinics on pat.clinic_id = clinics.clinic_id
        left join "datawarehouse".goodpill."patients_status_historic" as pme using (patient_id_cp)
        order by patient_id_cp, pme.event_date desc
    ),

    rh as (
        select
            patient_id_cp,
            rx_number,
            rx_id,
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
            rx_message_key,
            rx_message_text,
            rx_message_date,
            rx_sources,
            rx_gsn,
            drug_gsns as rx_drug_gsns,
            max_gsn as rx_max_gsn,
            refills_left as rx_refills_left,
            refills_original as rx_refills_original,
            refills_total as rx_refills_total,
            qty_left as rx_qty_left,
            qty_original as rx_qty_original,
            qty_total as rx_qty_total,
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
            rx_date_transferred_out,
            rx_date_transferred_in,
            provider_phone as provider_phone,
            clinic_name as rx_clinic_name,
            rx_added_first_at,
            rx_added_last_at,
            rx_date_changed,
            rx_date_expired,
            rx_date_added as rx_date_added,
            rx_stock_level_initial as rx_stock_level_initial,
            created_at as rx_created_at,
            updated_at as rx_updated_at,
            rx_group_created_at,
            rx_group_updated_at,
            rx_clinic_name_cp,
            status as rxs_single_status,
            rx_status_updated_at,
            provider_email as rx_provider_email,
            rx_inactivated_last_at,
            rx_activated_last_at,
            group_status as rx_group_status,
            rx_group_drug_generic,
            rx_group_drug_brand,
            rx_group_id,
            rx_group_drug_gsns,
            rx_group_rx_autofill,
            rx_group_refill_date_first,
            rx_group_refill_date_last,
            rx_group_refill_date_manual,
            rx_group_refill_date_default,
            rx_group_rx_date_changed,
            rx_group_rx_date_expired,
            transfer_pharmacy_phone as rx_transfer_pharmacy_phone,
            transfer_pharmacy_name as rx_transfer_pharmacy_name,
            transfer_pharmacy_fax as rx_transfer_pharmacy_fax,
            transfer_pharmacy_address as rx_transfer_pharmacy_address,
            sig_confirmed_by as rx_sig_confirmed_by,
            sig_confirmed_at as rx_sig_confirmed_at
        from "datawarehouse".goodpill."rxs_joined"
    ),

    oi as (
        select
            patient_id_cp,
            rx_number,
            line_id as item_line_id,
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
            unit_price_retail_initial as item_unit_price_retail_initial,
            unit_price_goodrx_initial as item_unit_price_goodrx_initial,
            unit_price_nadac_initial as item_unit_price_nadac_initial,
            unit_price_awp_initial as item_unit_price_awp_initial,
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
            days_and_message_updated_at as item_days_and_message_updated_at,
            days_and_message_initial_at as item_days_and_message_initial_at,
            days_pended as item_days_pended,
            qty_per_day_pended as item_qty_per_day_pended,
            refill_date_last as item_refill_date_last,
            refill_date_manual as item_refill_date_manual,
            refill_date_default as item_refill_date_default,
            add_user_id as item_add_user_id,
            chg_user_id as item_chg_user_id,
            count_lines as item_count_lines,
            repacked_by as item_repacked_by,
            unpended_at as item_unpended_at,
            pend_initial_at as item_pend_initial_at,
            pend_updated_at as item_pend_updated_at,
            ndc_pended as item_ndc_pended,
            drug_generic_pended as item_drug_generic_pended,
            filled_at as item_filled_at,
            pend_failed_at as item_pend_failed_at,
            filled_by as item_filled_by,
            pend_retried_by as item_pend_retried_by,
            status as item_status,
            pend_retried_days as item_pend_retried_days,
            pend_retried_at as item_pend_retried_at
        from "datawarehouse".goodpill."order_items"
    ),

    o as (
        select
            patient_id_cp,
            invoice_number as order_invoice_number,
            order_date_added as order_date_added,
            order_date_dispensed as order_date_dispensed,
            order_date_shipped as order_date_shipped,
            order_date_returned as order_date_returned,
            count_items as order_count_items,
            count_filled as order_count_filled,
            count_nofill as order_count_nofill,
            priority as order_priority,
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
            payment_default_updated_at as order_payment_default_updated_at,
            payment_actual_updated_at as order_payment_actual_updated_at,
            order_payment_coupon as order_payment_coupon,
            order_date_changed, --comes from Carepoint
            order_date_delivered,
            order_date_expedited,
            order_date_expected,
            order_date_expected_initial,
            order_date_failed,
            order_date_updated, --comes from Patient Portal
            order_stage_wc_updated_at,
            add_user_id as order_add_user_id,
            chg_user_id as order_chg_user_id,
            shipping_speed as order_shipping_speed,
            rx_group_removals_checked_at as order_rx_group_removals_checked_at,
            rx_group_additions_checked_at as order_rx_group_additions_checked_at,
            order_note as order_note,
            rph_check as order_rph_check,
            tech_fill as order_tech_fill,
            order_city as order_city,
            order_state as order_state,
            order_zip as order_zip,
            updated_at as order_updated_at,
            status as order_shipped_status,
            shipping_weight_oz_default as order_shipping_weight_oz_default,
            shipping_weight_oz_actual as order_shipping_weight_oz_actual,
            picking_expedited_at as order_picking_expedited_at
        from "datawarehouse".goodpill."orders"
    ),
    gp_pend_group as (
        select
        pend_group_name,
        invoice_number as pend_group_invoice_number,
        initial_pend_date as pend_group_initial_date,
        last_pend_date as pend_group_last_date
        from "datawarehouse".goodpill."gp_pend_group"
    ),
    o_safe as (
        select * from o
        left join gp_pend_group gpg on gpg.pend_group_invoice_number = o.order_invoice_number
    )

    select distinct on (patient_id_cp, rx_number, order_invoice_number)
        *,
        greatest(rh.rx_group_updated_at, rh.rx_group_created_at, oi.item_date_updated, o_safe.order_date_updated) as dw_updated_at
    from psh
    left join rh using (patient_id_cp)
    left join oi using (rx_number, patient_id_cp)
    full outer join o_safe using (order_invoice_number, patient_id_cp)
    order by
        patient_id_cp,
        rx_number,
        order_invoice_number,
        -- prioritize rxs which were updated before the order was dispensed
        rh.rx_group_updated_at <= o_safe.order_date_dispensed desc,
        rh.rx_group_updated_at desc,
        rh.rx_group_created_at <= o_safe.order_date_dispensed desc,
        rh.rx_group_created_at desc
),
drugs as (
    select
    drugs.drug_brand,
    drugs.drug_gsns,
    drugs.price30 as drug_price30,
    drugs.price90 as drug_price90,
    drugs.price_retail as drug_price_retail,
    drugs.price_goodrx as drug_price_goodrx,
    drugs.price_nadac as drug_price_nadac,
    drugs.qty_repack as drug_qty_repack,
    drugs.count_ndcs as drug_count_ndcs,
    drugs.drug_ordered,
    drugs.qty_min as drug_qty_min,
    drugs.days_min as drug_days_min,
    drugs.max_inventory as drug_max_inventory,
    drugs.message_display as drug_message_display,
    drugs.message_verified as drug_message_verified,
    drugs.message_destroyed as drug_message_destroyed,
    nullif(drug_generic, '') as drug_generic_name,
    coalesce(nullif(price_goodrx, 0), nullif(price_nadac, 0), nullif(price_retail, 0)) as drug_price_coalesced
    from "datawarehouse".goodpill."drugs"
),
providers as (
    select
    providers.first_name as provider_first_name,
	providers.last_name as provider_last_name,
	providers.verified as provider_verified,
    providers.npi as providers_npi,
    providers.provider_id as dw_provider_id,
    providers.first_rx_sent_date as provider_first_rx_sent_date,
    providers.last_rx_sent_date as provider_last_rx_sent_date
    from "datawarehouse".goodpill."providers"
),
dw_providers as (
    select
    dw_providers.provider_npi as dw_provider_npi,
    dw_providers.provider_name as dw_provider_name,
    dw_providers.provider_phone as dw_provider_phone,
    dw_providers.provider_id_sf as dw_provider_id_sf,
    dw_providers.default_clinic as dw_provider_default_clinic,
    dw_providers.default_clinic_imputed_at as dw_provider_default_clinic_imputed_at
    from "datawarehouse".goodpill."dw_providers"
),
clinics as (
    select
    clinics.clinic_id,
    clinics.clinic_name_cp,
    clinics.clinic_rx_date_added_first,
    clinics.clinic_rx_date_added_last,
    clinics.created_at as clinic_created_at,
    clinics.updated_at as clinic_updated_at
    from "datawarehouse".goodpill."clinics"
),
dw_clinics as (
    select
    dw_clinics.clinic_id as dw_clinic_id,
    dw_clinics.clinic_group_id as dw_clinic_group_id,
    dw_clinics.clinic_name as dw_clinic_name,
    dw_clinics.clinic_address as dw_clinic_address,
    dw_clinics.clinic_street as dw_clinic_street,
    dw_clinics.clinic_city as dw_clinic_city,
    dw_clinics.clinic_state as dw_clinic_state,
    dw_clinics.clinic_zip as dw_clinic_zip,
    dw_clinics.clinic_phone as dw_clinic_phone,
    dw_clinics.clinic_id_sf as dw_clinic_id_sf,
    dw_clinics.created_at as dw_clinic_created_at,
    dw_clinics.updated_at as dw_clinic_updated_at
    from "datawarehouse".goodpill."dw_clinics"
),
dw_clinic_groups as (
    select
    dw_clinic_groups.clinic_group_id as dw_clinic_groups_id,
    dw_clinic_groups.clinic_group_name as dw_clinic_group_name,
    dw_clinic_groups.clinic_group_id_sf as dw_clinic_group_id_sf,
    dw_clinic_groups.clinic_group_domain as dw_clinic_group_domain,
    dw_clinic_groups.created_at as dw_clinic_groups_created_at,
    dw_clinic_groups.updated_at as dw_clinic_groups_updated_at
    from "datawarehouse".goodpill."dw_clinic_groups"
),
patients as (
    select
    patient_id_cp,
    patient_id_wc,
    patient_date_registered,
    patient_date_reviewed,
    patient_date_added,
    patient_date_changed,
    patient_date_updated,
    first_name as patient_first_name,
    last_name as patient_last_name,
    birth_date as patient_birth_date,
    birth_date as patient_language,
    phone1 as patient_phone1,
    phone2 as patient_phone2,
    concat(patient_address1, ', ', patient_address2) as patient_address,
    patient_city as patient_city,
    patient_state as patient_state,
    patient_zip as patient_zip,
    payment_card_type as patient_payment_card_type,
    payment_card_last4 as patient_payment_card_last4,
    payment_card_date_expired as patient_payment_card_date_expired,
    payment_card_autopay as patient_payment_card_autopay,
    payment_method_default as patient_payment_method_default,
    patient_date_first_rx_received,
    patient_date_first_dispensed,
    patient_date_first_expected_by,
    refills_used as patient_refills_used,
    email as patient_email,
    patient_autofill,
    initial_invoice_number as patient_initial_invoice_number,
    patient_note,
    allergies_none as patient_allergies_none,
    allergies_cephalosporins as patient_allergies_cephalosporins,
    allergies_sulfa as patient_allergies_sulfa,
    allergies_aspirin as patient_allergies_aspirin,
    allergies_penicillin as patient_allergies_penicillin,
    allergies_erythromycin as patient_allergies_erythromycin,
    allergies_codeine as patient_allergies_codeine,
    allergies_nsaids as patient_allergies_nsaids,
    allergies_salicylates as patient_allergies_salicylates,
    allergies_azithromycin as patient_allergies_azithromycin,
    allergies_amoxicillin as patient_allergies_amoxicillin,
    allergies_tetracycline as patient_allergies_tetracycline,
    allergies_other as patient_allergies_other,
    medications_other as patient_medications_other,
    pharmacy_npi as pharmacy_npi,
    pharmacy_name as pharmacy_name,
    pharmacy_phone as pharmacy_phone,
    pharmacy_fax as pharmacy_fax,
    pharmacy_address as pharmacy_address,
    patient_inactive,
    payment_coupon as patient_payment_coupon,
    tracking_coupon as patient_tracking_coupon,
    patient_deleted as patient_patient_deleted,
    third_party_id as patient_third_party_id,
    terms_viewed_at as patient_terms_viewed_at,
    terms_accepted as patient_terms_accepted
    from "datawarehouse".goodpill."patients"
),
gp_stock_live as (
    select
    drug_generic as stock_live_drug_generic,
    price_per_month as stock_live_price_per_month,
    drug_ordered as stock_live_drug_ordered,
    qty_repack as stock_live_qty_repack,
    months_inventory as stock_live_months_inventory,
    avg_inventory as stock_live_avg_inventory,
    last_inventory as stock_live_last_inventory,
    months_entered as stock_live_months_entered,
    stddev_entered as stock_live_stddev_entered,
    total_entered as stock_live_total_entered,
    months_dispensed as stock_live_months_dispensed,
    stddev_dispensed_actual as stock_live_stddev_dispensed_actual,
    total_dispensed_actual as stock_live_total_dispensed_actual,
    total_dispensed_default as stock_live_total_dispensed_default,
    stddev_dispensed_default as stock_live_stddev_dispensed_default,
    month_interval as stock_live_month_interval,
    default_rxs_min as stock_live_default_rxs_min,
    last_inv_low_threshold as stock_live_last_inv_low_threshold,
    last_inv_high_threshold as stock_live_last_inv_high_threshold,
    last_inv_onetime_threshold as stock_live_last_inv_onetime_threshold,
    zlow_threshold as stock_live_zlow_threshold,
    zhigh_threshold as stock_live_zhigh_threshold,
    zscore as stock_live_zscore,
    stock_level as stock_live_level
    from "datawarehouse".goodpill."gp_stock_live"
),
drugs_safe as (
    -- pre join
    select * from drugs
    left join gp_stock_live gsl on drugs.drug_generic_name = gsl.stock_live_drug_generic
),
clinics_safe as (
    -- pre join
    select * from clinics
    left join dw_clinics on dw_clinics.dw_clinic_id = clinics.clinic_id
    left join dw_clinic_groups on dw_clinic_groups.dw_clinic_groups_id = dw_clinics.dw_clinic_group_id
),
providers_safe as (
    -- pre join
    select * from providers
    left join dw_providers on dw_providers.dw_provider_npi = providers.providers_npi
)
select *
from goodpill_snapshot as gds
left join drugs_safe on drugs_safe.drug_generic_name = gds.rx_drug_generic
left join patients using (patient_id_cp)
left join providers_safe on providers_safe.providers_npi = gds.rx_provider_npi
left join clinics_safe on clinics_safe.clinic_name_cp = gds.rx_clinic_name