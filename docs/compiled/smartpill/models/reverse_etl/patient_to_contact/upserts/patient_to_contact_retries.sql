select *
from "datawarehouse".reverse_etl."patient_to_contact_editions"
where
gp_patient_id_cp__c in (
	select patient_id_cp
	from "datawarehouse"."reverse_etl"."patient_to_contact_non_updated"
	where its_fixed = false
	and patient_id_cp is not null
)
OR "Id" in (
	select salesforce_contact_id
	from "datawarehouse"."reverse_etl"."patient_to_contact_non_updated"
	where its_fixed = false
	and salesforce_contact_id is not null
)

UNION

select *
from "datawarehouse".reverse_etl."patient_to_contact_additions"
where
gp_patient_id_cp__c in (
	select patient_id_cp
	from "datawarehouse"."reverse_etl"."patient_to_contact_non_updated"
	where its_fixed = false
	and patient_id_cp is not null
)