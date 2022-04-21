select
	gs.*,
	cg.meta_group as clinic_meta_group,
	cg.meta_template as clinic_meta_template,
	drg.brand_name as drug_brand,
	drg.price30 as drug_price30,
	drg.price90 as drug_price90,
	drg.price_retail as drug_price_retail,
	drg.price_goodrx as drug_price_goodrx,
	drg.price_nadac as drug_price_nadac,
	drg.price_coalesced as drug_price_coalesced,
	loc.city as location_city,
	loc.state as location_state,
	loc.zip_code as location_zip_code,
	pat.patient_date_registered as patient_date_registered,
	pat.patient_date_added as patient_date_added,
	pat.fill_next as patient_fill_next,
	pat.days_overdue as patient_days_overdue,
	pat.first_name as patient_first_name,
	pat.last_name as patient_last_name,
	pat.birth_date as patient_birth_date,
	pat.phone1 as patient_phone1,
	pat.phone2 as patient_phone2,
	pat.patient_address as patient_address,
	pat.patient_city as patient_city,
	pat.patient_state as patient_state,
	pat.patient_zip as patient_zip,
	pat.payment_card_type as patient_payment_card_type,
	pat.payment_card_last4 as patient_payment_card_last4,
	pat.payment_card_date_expired as patient_payment_card_date_expired,
	pat.payment_method_default as patient_payment_method_default,
	pat.refills_used as patient_refills_used,
	pat.date_first_rx_received as patient_date_first_rx_received,
	pat.date_first_dispensed as patient_date_first_dispensed,
	pat.date_first_expected_by as patient_date_first_expected_by,
	phr.pharmacy_npi,
    phr.pharmacy_name,
    phr.pharmacy_phone,
    phr.pharmacy_fax,
    phr.pharmacy_address,
	prv.provider_phone
from "datawarehouse".dev_analytics."goodpill_snapshot" gs
left join "datawarehouse".dev_analytics."drugs" drg on gs.rx_drug_generic = drg.generic_name
left join "datawarehouse".dev_analytics."locations" loc on gs.order_location_id = loc.id
left join "datawarehouse".dev_analytics."providers" prv on gs.rx_provider_npi = prv.provider_npi
left join "datawarehouse".dev_analytics."patients" pat on gs.patient_id_cp = pat.patient_id_cp
left join "datawarehouse".dev_analytics."pharmacies" phr on pat.pharmacy_id = phr.pharmacy_id
left join "datawarehouse".dev_analytics."clinics_groups" cg on gs.clinic_coalesced_name ilike concat('%', cg.meta_group, '%')