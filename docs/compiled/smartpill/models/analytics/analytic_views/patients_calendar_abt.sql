select
	pc.patient_id_cp,
	pc.date_day,
	ped.event_name as event_name_day,
	pew.event_name as event_name_week,
	pem.event_name as event_name_month,
	pey.event_name as event_name_year,
	p."patient_date_registered" as "patient_date_registered",
  p."patient_date_added" as "patient_date_added",
  p."fill_next" as "patient_fill_next",
  p."days_overdue" as "patient_days_overdue",
  p."first_name" as "patient_first_name",
  p."last_name" as "patient_last_name",
  p."birth_date" as "patient_birth_date",
  p."phone1" as "patient_phone1",
  p."phone2" as "patient_phone2",
  p."patient_address" as "patient_address",
  p."patient_city" as "patient_city",
  p."patient_state" as "patient_state",
  p."patient_zip" as "patient_zip",
  p."payment_card_type" as "patient_payment_card_type",
  p."payment_card_last4" as "patient_payment_card_last4",
  p."payment_card_date_expired" as "patient_payment_card_date_expired",
  p."payment_method_default" as "patient_payment_method_default",
  p."payment_coupon" as "patient_payment_coupon",
  p."tracking_coupon" as "patient_tracking_coupon",
  p."date_first_rx_received" as "patient_date_first_rx_received",
  p."date_first_dispensed" as "patient_date_first_dispensed",
  p."date_first_expected_by" as "patient_date_first_expected_by",
  p."refills_used" as "patient_refills_used",
  p."pharmacy_id" as "patient_pharmacy_id",
  p."date_processed" as "patient_date_processed"
from "datawarehouse".analytics."patients_calendar" pc
inner join "datawarehouse".analytics."patients" p using (patient_id_cp)
inner join "datawarehouse".analytics."patient_events" ped on pc.event_weight_day = ped.event_weight
inner join "datawarehouse".analytics."patient_events" pew on pc.event_weight_week = pew.event_weight
inner join "datawarehouse".analytics."patient_events" pem on pc.event_weight_month = pem.event_weight
inner join "datawarehouse".analytics."patient_events" pey on pc.event_weight_year = pey.event_weight