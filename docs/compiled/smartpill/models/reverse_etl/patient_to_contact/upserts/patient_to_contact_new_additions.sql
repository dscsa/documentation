select *
from "datawarehouse".reverse_etl."patient_to_contact_additions"
where
gp_patient_id_cp__c not in (
	select patient_id_cp
	from "datawarehouse"."reverse_etl"."patient_to_contact_non_updated"
	where its_fixed = false
	and patient_id_cp is not null
)
and (
	(lower("FirstName") || ' ' || lower("LastName")) not like '%test%' and
	(lower("FirstName") || ' ' || lower("LastName")) not like '%fake%' and
	(lower("FirstName") || ' ' || lower("LastName")) not like '%user%'
)