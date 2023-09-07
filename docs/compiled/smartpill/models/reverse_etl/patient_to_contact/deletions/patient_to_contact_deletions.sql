with
updated_tasks as (
    select task_id, task_contact_id from "datawarehouse".salesforce."patients_task" pt
	where task_is_deleted = false
),
tasks_to_migrate as (
	select * from "datawarehouse".reverse_etl."patient_to_contact_tasks_to_migrate"
	where task_id is not null
),
tasks_to_migrate_failures as (
	select
		tm.*,
		SUM (CASE WHEN pt.task_contact_id is distinct from tm.target_patient_contact_id THEN 1 ELSE 0 END)
			OVER(PARTITION BY tm.target_patient_contact_id) as migration_failure_sum
	from tasks_to_migrate tm
	left join updated_tasks pt using (task_id)
),
deletions as (
	select distinct source_patient_contact_id as "Id"
	from tasks_to_migrate_failures
	where migration_failure_sum = 0

	UNION

	-- patients to delete with no task assigned
	select distinct source_patient_contact_id as "Id"
	from "datawarehouse".reverse_etl."patient_to_contact_tasks_to_migrate"
	where task_id is null

),
final as (
    SELECT
        d."Id",
		CONCAT(
			'Deleted patient contact information:', E'\n',
			'contact name: ' , coalesce(contact_name, '[null]') , E'\n',
			'salesforce id: ' , coalesce(contact_id, '[null]') , E'\n',
			'patient_id_cp: ' , coalesce(contact_gp_patient_id_cp__c::varchar, '[null]') , E'\n',
			'email: ' , coalesce(contact_gp_email__c, contact_email, '[null]') , E'\n' ,
			'patient_address1: ' , coalesce(contact_gp_patient_address1__c, '[null]') , E'\n' ,
			'patient_address2: ' , coalesce(contact_gp_patient_address2__c, '[null]') , E'\n' ,
			'patient_city: ' , coalesce(contact_gp_patient_city__c, '[null]') , E'\n' ,
			'patient_state: ' , coalesce(contact_gp_patient_state__c, '[null]') , E'\n' ,
			'patient_zip: ' , coalesce(contact_gp_patient_zip__c, '[null]') , E'\n' ,
			'phone1: ' , coalesce(contact_gp_phone1__c, '[null]') , E'\n' ,
			'phone2: ' , coalesce(contact_gp_phone2__c, '[null]')
		) as "Description"
    FROM deletions d
    LEFT JOIN "datawarehouse".salesforce."patients_contact" pc ON d."Id" = pc.contact_id
	where pc.contact_isdeleted = false
)
select * from final