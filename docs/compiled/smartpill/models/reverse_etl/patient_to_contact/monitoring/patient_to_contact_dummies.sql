with dummies as (
    select
        contact_id
    from "datawarehouse".salesforce."patients_contact" pc
    where
        -- (
        --     contact_name ~ '\d{4}-\d{2}-\d{2}'
        --     or contact_birthdate is not null
        -- )
        contact_gp_patient_id_cp__c is null
        and contact_isdeleted = false
        and contact_last_modified_date < (now() - interval '30' DAY)
        AND to_char(contact_created_date, 'YYYY-MM-DD') > ''
        and (
            lower(contact_lastname) not like '%test%'
            and lower(contact_name) not like '%test%'
            and lower(contact_name) not like '%fake%'
            and lower(contact_name) not like '%user%'
            and lower(contact_email) not like '%test%'
        )
        and contact_id not in (
            select "Id" from "datawarehouse"."reverse_etl"."contact_notifications_history"
            where notif_type = 'DUMMY'
        )
        and contact_account_id = '0011M00002Mnf3QQAR' -- Good Pill Home Delivery
),
final as (
    select
        null as "Id", -- create new task
        contact_id as "WhoId",
        'Salesforce patient not matched to Carepoint patient' as "Subject",
        'This Salesforce patient has no CP ID and was created 30 days ago. Please validate if the patient exists in CP and WC.' as "Description",
        'a001M00000aT1igQAC' as "WhatId",
        'a001M00000aT1igQAC' as "Assigned_To__c"
    from dummies
)
select * from final