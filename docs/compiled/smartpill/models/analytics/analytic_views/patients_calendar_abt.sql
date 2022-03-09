select
	pc.patient_id_cp,
	pc.date_day,
	ped.event_name as event_name_day,
	pew.event_name as event_name_week,
	pem.event_name as event_name_month,
	pey.event_name as event_name_year,
	
from "datawarehouse".prod_analytics."patients_calendar" pc
inner join "datawarehouse".prod_analytics."patients" p using (patient_id_cp)
inner join "datawarehouse".prod_analytics."patient_events" ped on pc.event_weight_day = ped.event_weight
inner join "datawarehouse".prod_analytics."patient_events" pew on pc.event_weight_week = pew.event_weight
inner join "datawarehouse".prod_analytics."patient_events" pem on pc.event_weight_month = pem.event_weight
inner join "datawarehouse".prod_analytics."patient_events" pey on pc.event_weight_year = pey.event_weight