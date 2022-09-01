with qb as (
	select
		amount as qbo_amount,
		account_sub as qbo_account_sub,
		account_full as qbo_account_full,
		account_top as qbo_account_top,
		account_type_top as qbo_account_type_top,
		account_type_sub as qbo_account_type_sub,
		account_number as qbo_account_number,
		transaction_date as qbo_transaction_date
	from "datawarehouse".dev_quickbooks."general_ledger_abt"
)

select
	order_invoice_number,
	patient_id_cp,
	rx_number,
	dw_patient_status,
	coalesce(patient_date_active, qbo_transaction_date) as patient_date_active,
	coalesce(patient_date_no_rx, qbo_transaction_date) as patient_date_no_rx,
	coalesce(patient_date_unregistered, qbo_transaction_date) as patient_date_unregistered,
	coalesce(patient_date_deceased, qbo_transaction_date) as patient_date_deceased,
	coalesce(patient_date_churned_no_fillable_rx, qbo_transaction_date) as patient_date_churned_no_fillable_rx,
	coalesce(patient_date_inactive, qbo_transaction_date) as patient_date_inactive,
	coalesce(patient_date_churned_other, qbo_transaction_date) as patient_date_churned_other,
	rx_numbers,
	rx_best_rx_number,
	rx_provider_npi,
	rx_drug_generic,
	rx_drug_brand,
	rx_drug_name,
	rx_message_key,
	rx_message_text,
	rx_gsn,
	rx_drug_gsns,
	rx_max_gsn,
	rx_refills_left,
	rx_refills_original,
	rx_refills_total,
	rx_qty_left,
	rx_qty_original,
	rx_sig_actual,
	rx_sig_initial,
	rx_sig_clean,
	rx_sig_qty,
	rx_sig_v1_qty,
	rx_sig_v1_days,
	rx_sig_v1_qty_per_day,
	rx_sig_days,
	rx_sig_qty_per_day,
	rx_sig_qty_per_day_default,
	rx_sig_qty_per_day_actual,
	rx_sig_durations,
	rx_sig_qtys_per_time,
	rx_sig_frequencies,
	rx_sig_frequency_numerators,
	rx_sig_frequency_denominators,
	rx_sig_v2_qty,
	rx_sig_v2_days,
	rx_sig_v2_qty_per_day,
	rx_sig_v2_unit,
	rx_sig_v2_conf_score,
	rx_sig_v2_dosages,
	rx_sig_v2_scores,
	rx_sig_v2_frequencies,
	rx_sig_v2_durations,
	rx_autofill,
	coalesce(rx_refill_date_first, qbo_transaction_date) as rx_refill_date_first,
	coalesce(rx_refill_date_last, qbo_transaction_date) as rx_refill_date_last,
	coalesce(rx_refill_date_manual, qbo_transaction_date) as rx_refill_date_manual,
	coalesce(rx_refill_date_next, qbo_transaction_date) as rx_refill_date_next,
	rx_status,
	rx_stage,
	rx_source,
	rx_transfer,
	coalesce(rx_date_transferred, qbo_transaction_date) as rx_date_transferred,
	provider_first_name,
	provider_last_name,
	provider_phone,
	rx_clinic_name,
	coalesce(rx_date_changed, qbo_transaction_date) as rx_date_changed,
	coalesce(rx_date_expired, qbo_transaction_date) as rx_date_expired,
	coalesce(rx_date_added, qbo_transaction_date) as rx_date_added,
	rx_created_at,
	rx_updated_at,
	group_created_at,
	item_groups,
	item_rx_dispensed_id,
	item_stock_level_initial,
	item_rx_message_keys_initial,
	item_patient_autofill_initial,
	item_rx_autofill_initial,
	item_rx_numbers_initial,
	item_zscore_initial,
	item_refills_dispensed_default,
	item_refills_dispensed_actual,
	item_days_dispensed_default,
	item_days_dispensed_actual,
	item_qty_dispensed_default,
	item_qty_dispensed_actual,
	item_price_dispensed_default,
	item_price_dispensed_actual,
	item_qty_pended_total,
	item_qty_pended_repacks,
	item_count_pended_total,
	item_count_pended_repacks,
	item_message_keys,
	item_message_text,
	item_type,
	item_added_by,
	coalesce(item_date_added, qbo_transaction_date) as item_date_added,
	coalesce(item_date_changed, qbo_transaction_date) as item_date_changed,
	coalesce(item_date_updated, qbo_transaction_date) as item_date_updated,
	coalesce(item_refill_date_last, qbo_transaction_date) as item_refill_date_last,
	coalesce(item_refill_date_manual, qbo_transaction_date) as item_refill_date_manual,
	coalesce(item_refill_date_default, qbo_transaction_date) as item_refill_date_default,
	coalesce(item_refill_target_date, qbo_transaction_date) as item_refill_target_date,
	item_refill_target_days,
	item_refill_target_rxs,
	item_add_user_id,
	item_chg_user_id,
	item_count_lines,
	item_repacked_by,
	item_repacked_at,
	patient_id_wc,
	coalesce(order_date_added, qbo_transaction_date) as order_date_added,
	coalesce(order_date_dispensed, qbo_transaction_date) as order_date_dispensed,
	coalesce(order_date_shipped, qbo_transaction_date) as order_date_shipped,
	coalesce(order_date_returned, qbo_transaction_date) as order_date_returned,
	order_count_items,
	order_count_filled,
	order_count_nofill,
	order_source,
	order_stage_cp,
	order_stage_wc,
	order_status,
	order_address1,
	order_address2,
	order_invoice_doc_id,
	order_tracking_number,
	order_payment_total_default,
	order_payment_total_actual,
	order_payment_fee_default,
	order_payment_fee_actual,
	order_payment_due_default,
	order_payment_due_actual,
	order_payment_date_autopay,
	order_payment_method_actual,
	order_payment_coupon,
	coalesce(order_date_delivered, qbo_transaction_date) as order_date_delivered,
	coalesce(order_date_expedited, qbo_transaction_date) as order_date_expedited,
	coalesce(order_date_expected, qbo_transaction_date) as order_date_expected,
	coalesce(order_date_expected_initial, qbo_transaction_date) as order_date_expected_initial,
	coalesce(order_date_failed, qbo_transaction_date) as order_date_failed,
	order_add_user_id,
	order_chg_user_id,
	order_shipping_speed,
	order_note,
	order_rph_check,
	order_tech_fill,
	order_city,
	order_state,
	order_zip,
	coalesce(order_date_updated, qbo_transaction_date) as order_date_updated,
	dw_updated_at,
	coalesce(clinic_rx_date_added_first, qbo_transaction_date) as clinic_rx_date_added_first,
	coalesce(clinic_rx_date_added_last, qbo_transaction_date) as clinic_rx_date_added_last,
	clinic_created_at,
	clinic_updated_at,
	dw_clinic_id,
	dw_clinic_name,
	dw_clinic_name_cp,
	dw_clinic_address,
	dw_clinic_street,
	dw_clinic_city,
	dw_clinic_state,
	dw_clinic_zip,
	dw_clinic_phone,
	dw_clinic_id_sf,
	dw_clinic_created_at,
	dw_clinic_updated_at,
	dw_clinic_group_id,
	dw_clinic_group_name,
	dw_clinic_group_id_sf,
	dw_clinic_group_domain,
	dw_clinic_groups_created_at,
	dw_clinic_groups_updated_at,
	drug_brand,
	drug_gsns,
	drug_price30,
	drug_price90,
	drug_price_retail,
	drug_price_goodrx,
	drug_price_nadac,
	drug_price_coalesced,
	drug_qty_repack,
	drug_count_ndcs,
	dw_provider_id,
	coalesce(provider_first_rx_sent_date, qbo_transaction_date) as provider_first_rx_sent_date,
	coalesce(provider_last_rx_sent_date, qbo_transaction_date) as provider_last_rx_sent_date,
	dw_provider_name,
	dw_provider_phone,
	dw_provider_id_sf,
	coalesce(patient_date_registered, qbo_transaction_date) as patient_date_registered,
	coalesce(patient_date_added, qbo_transaction_date) as patient_date_added,
	patient_first_name,
	patient_last_name,
	coalesce(patient_birth_date, qbo_transaction_date) as patient_birth_date,
	patient_language,
	patient_phone1,
	patient_phone2,
	patient_address,
	patient_city,
	patient_state,
	patient_zip,
	patient_payment_card_type,
	patient_payment_card_last4,
	coalesce(patient_payment_card_date_expired, qbo_transaction_date) as patient_payment_card_date_expired,
	patient_payment_card_autopay,
	patient_payment_method_default,
	coalesce(patient_date_first_rx_received, qbo_transaction_date) as patient_date_first_rx_received,
	coalesce(patient_date_first_dispensed, qbo_transaction_date) as patient_date_first_dispensed,
	coalesce(patient_date_first_expected_by, qbo_transaction_date) as patient_date_first_expected_by,
	patient_refills_used,
	patient_email,
	patient_autofill,
	patient_initial_invoice_number,
	patient_note,
	patient_allergies_none,
	patient_allergies_cephalosporins,
	patient_allergies_sulfa,
	patient_allergies_aspirin,
	patient_allergies_penicillin,
	patient_allergies_erythromycin,
	patient_allergies_codeine,
	patient_allergies_nsaids,
	patient_allergies_salicylates,
	patient_allergies_azithromycin,
	patient_allergies_amoxicillin,
	patient_allergies_tetracycline,
	patient_allergies_other,
	patient_medications_other,
	pharmacy_npi,
	pharmacy_name,
	pharmacy_phone,
	pharmacy_fax,
	pharmacy_address,
	patient_payment_coupon,
	patient_tracking_coupon,
	qbo_amount,
	qbo_account_sub,
	qbo_account_full,
	qbo_account_top,
	qbo_account_type_top,
	qbo_account_type_sub,
	qbo_account_number,
	qbo_transaction_date
from
	"datawarehouse".dev_analytics."goodpill_snapshot_abt" gsa
	full outer join qb on false