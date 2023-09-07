with deletions as (
    select contact_id, contact_gp_patient_id_cp__c
    from "datawarehouse".salesforce."patients_contact" pc
    left join "datawarehouse".goodpill."patients" p on pc.contact_gp_patient_id_cp__c = p.patient_id_cp
    where
        p.patient_id_cp is null
        AND contact_gp_patient_id_cp__c is not null
        AND to_char(contact_created_date, 'YYYY-MM-DD') > ''
        and pc.contact_isdeleted = false
        and (
            lower(contact_lastname) not like '%test%'
            and lower(contact_name) not like '%test%'
            and lower(contact_name) not like '%fake%'
            and lower(contact_name) not like '%user%'
            and lower(contact_email) not like '%test%'
        )
        and contact_id not in (
            select "Id" from "datawarehouse"."reverse_etl"."contact_notifications_history"
            where notif_type = 'NON MERGED'
        )
        and contact_account_id = '0011M00002Mnf3QQAR' -- Good Pill Home Delivery
),
deletions_without_merge as (
    select contact_id
    from deletions d
    left join "datawarehouse".goodpill."patient_merge_cp" pm ON d.contact_gp_patient_id_cp__c = pm.source_patient_id_cp
    where pm.source_patient_id_cp is null --the deletion was not merged
),
final as (
    select
        null as "Id", -- create new task
        contact_id as "WhoId",
        'Carepoint patient deleted BUT not due to a merge action' as "Subject",
        'This patient exists in Salesforce but can not be found in Carepoint matching by CP ID. It was NOT deleted by a merge action either (according to the gp_patient_merge_cp table).\nPlease validate if the patient exists in CP and WC.' as "Description",
        'a001M00000aT1igQAC' as "WhatId",
        'a001M00000aT1igQAC' as "Assigned_To__c"
    from deletions_without_merge
)
select * from final