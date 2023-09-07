with patient_merge as (
	select distinct source_patient_id_cp, target_patient_id_cp
    from "datawarehouse".goodpill."patient_merge_cp"
	where
	source_patient_id_cp is distinct from target_patient_id_cp --skip patients merged to itself-- feature flag
		AND source_patient_id_cp = --	union
--	select distinct *
--  from ref('patient_merge_wc')
),
patients_contact_safe as (
	select * from "datawarehouse".salesforce."patients_contact"
	where contact_isdeleted = false
),
patient_merge_safe as (
	select
        pm.*,
        pc_source.contact_id as source_patient_contact_id,
        pc_target.contact_id as target_patient_contact_id
	from patient_merge pm
	left join patients_contact_safe pc_source on pm.source_patient_id_cp = pc_source.contact_gp_patient_id_cp__c
	left join patients_contact_safe pc_target on pm.target_patient_id_cp = pc_target.contact_gp_patient_id_cp__c
	left join "datawarehouse".goodpill."patients" p_source on pm.source_patient_id_cp = p_source.patient_id_cp
	left join "datawarehouse".goodpill."patients" p_target on pm.target_patient_id_cp = p_target.patient_id_cp
	where
	p_source.patient_id_cp is null and -- its deleted from goodpill
	p_target.patient_id_cp is not null -- target patient exists in goodpill
	and (
		pc_source.contact_gp_patient_id_cp__c is not null and
		pc_target.contact_gp_patient_id_cp__c is not null
	) -- both patients exists in saleforce. (we can migrate tasks or its ok deleting one of the two.)
),
tasks_with_id_cp as (
    select task_id, contact_gp_patient_id_cp__c
    from "datawarehouse".salesforce."patients_task" pt
    inner join patients_contact_safe pc on pc.contact_id = pt.task_contact_id
	where task_is_deleted = false
),
final as (
    select pm.*, task_id
    from patient_merge_safe pm
    left join tasks_with_id_cp pt on pm.source_patient_id_cp = pt.contact_gp_patient_id_cp__c
	--left join because I want to keep patient with no tasks assigned, but needs to be deleted.
)
select distinct * from final