
with task as (
	select *
    from "datawarehouse".salesforce."donors_task"
),
contact as (
	select *
    from "datawarehouse".salesforce."donors_contact" as c
    where
        contact_isdeleted = false
        and c.contact_firstname not like '%Test%'
        and c.contact_firstname not like '%test%'
        and c.contact_name not like '%Test%'
        and c.contact_name not like '%test%'
),
gp_user as (
    select
        Id as gp_user_id,
        OwnerId as gp_user_ownerid,
        Name as gp_user_name,
        Role__c as gp_user_role__c,
        Email__c as gp_user_email__c,
        gp_user_airbyte_emitted_at,
        isdeleted as gp_user_isdeleted
    from "datawarehouse".salesforce."patients_gp_user__c"
    where isdeleted = false
),
donors_user as (
	select *
    from "datawarehouse".salesforce."donors_user"
    where user_isdeleted = false
),
final as (
    select
    *,
    greatest(
        task.task_airbyte_emitted_at,
        contact.contact_airbyte_emitted_at,
        gp_user.gp_user_airbyte_emitted_at,
        donors_user.user_airbyte_emitted_at
    ) as _airbyte_emitted_at

    from task
    left join contact on contact.contact_id = task.task_contact_id
    left join gp_user on task.task_what_id = gp_user.gp_user_Id
    left join donors_user on task.task_user_id = donors_user.user_id
)
select * from final

where _airbyte_emitted_at > (select max(_airbyte_emitted_at) from "datawarehouse".salesforce."salesforce_donors_abt")
