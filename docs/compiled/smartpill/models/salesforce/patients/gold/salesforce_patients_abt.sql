
with task as (
	select *
    from "datawarehouse".salesforce."patients_task"
    where task_account_id = '0011M00002Mnf3QQAR' -- Good Pill Home Delivery
),
contact as (
	select *
    from "datawarehouse".salesforce."patients_contact" as c
    where
        c.contact_gp_patient_id_cp__c is not null
        and c.contact_isdeleted = false
        and contact_isdeleted = false
        and c.contact_firstname not like '%Test%'
        and c.contact_firstname not like '%test%'
        and c.contact_name not like '%Test%'
        and c.contact_name not like '%test%'
        and c.contact_account_id = '0011M00002Mnf3QQAR' -- Good Pill Home Delivery
),
gp_user as (
    select
        Id as gp_user_id,
        OwnerId as gp_user_ownerid,
        Name as gp_user_name,
        Role__c as gp_user_role__c,
        Email__c as gp_user_email__c,
        gp_user_airbyte_emitted_at
    from "datawarehouse".salesforce."patients_gp_user__c"
    where isdeleted = false
),
gp_user_assigned_to as (
	select * from gp_user
),
gp_user_related_to as (
	select * from gp_user
),
tasks_x_contact as (
    select *
    from task
    full outer join contact on contact.contact_id = task.task_contact_id
),
final as (
    select
    tasks_x_contact.*,

	gp_user_assigned_to.gp_user_id as gp_user_assigned_to_id,
	gp_user_assigned_to.gp_user_ownerid as gp_user_assigned_to_ownerid,
    gp_user_assigned_to.gp_user_name as gp_user_assigned_to_name,
	gp_user_assigned_to.gp_user_role__c as gp_user_assigned_to_role__c,
	gp_user_assigned_to.gp_user_email__c as gp_user_assigned_to_email__c,

    gp_user_related_to.gp_user_id as gp_user_related_to_id,
	gp_user_related_to.gp_user_ownerid as gp_user_related_to_ownerid,
    gp_user_related_to.gp_user_name as gp_user_related_to_name,
	gp_user_related_to.gp_user_role__c as gp_user_related_to_role__c,
	gp_user_related_to.gp_user_email__c as gp_user_related_to_email__c,

    greatest(
        tasks_x_contact.task_airbyte_emitted_at,
        tasks_x_contact.contact_airbyte_emitted_at,
        gp_user_assigned_to.gp_user_airbyte_emitted_at,
        gp_user_related_to.gp_user_airbyte_emitted_at
    ) as _airbyte_emitted_at

    from tasks_x_contact
    left join gp_user_assigned_to on tasks_x_contact.task_assigned_to__c = gp_user_assigned_to.gp_user_id
    left join gp_user_related_to on tasks_x_contact.task_what_id = gp_user_related_to.gp_user_Id
)
select * from final

where final._airbyte_emitted_at > (select max(t._airbyte_emitted_at) from "datawarehouse".salesforce."salesforce_patients_abt" t)
