WITH deletions as (
    SELECT d."Id" as contact_id, d."Description"
    FROM "datawarehouse".reverse_etl."patient_to_contact_deletions" d
    LEFT JOIN "datawarehouse".salesforce."patients_contact" pc ON d."Id" = pc.contact_id
    WHERE pc.contact_isdeleted = true -- its deleted from Salesforce
),
patient_merge_cp_x_contacts as (
    SELECT distinct
        source_patient_id_cp,
        source_patient_contact_id,
        target_patient_id_cp,
        target_patient_contact_id
    FROM "datawarehouse".reverse_etl."patient_to_contact_tasks_to_migrate"
)
SELECT
    null as "Id", --create new task
    pm_x_c.target_patient_contact_id as "WhoId",
    d."Description",
    'Salesforce patient contact merged with existing contact' as "Subject",
    'a001M00000aT1igQAC' as "Assigned_To__c", --.Testing
    'a001M00000aT1igQAC' as "WhatId", --.Testing
    'Completed' as "Status",
    to_char(now(), 'YYYY-MM-dd') as "ActivityDate"
FROM deletions d
LEFT JOIN patient_merge_cp_x_contacts pm_x_c ON d.contact_id = pm_x_c.source_patient_contact_id