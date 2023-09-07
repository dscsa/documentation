select *
from "datawarehouse".reverse_etl."patient_to_contact_editions"
where
gp_patient_id_cp__c not in (
	select patient_id_cp
	from "datawarehouse"."reverse_etl"."patient_to_contact_non_updated"
	where its_fixed = false
	and patient_id_cp is not null
)
and "Id" not in (
	select salesforce_contact_id
	from "datawarehouse"."reverse_etl"."patient_to_contact_non_updated"
	where its_fixed = false
	and salesforce_contact_id is not null
)
and (
	(lower("FirstName") || ' ' || lower("LastName")) not like '%test%' and
	(lower("FirstName") || ' ' || lower("LastName")) not like '%fake%' and
	(lower("FirstName") || ' ' || lower("LastName")) not like '%user%'
)