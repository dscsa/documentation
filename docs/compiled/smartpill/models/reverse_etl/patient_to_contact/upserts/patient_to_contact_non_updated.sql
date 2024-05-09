WITH
--IDS TO UPDATE
ids_to_update_table as (
    SELECT "Id" as id, gp_email__c, gp_patient_id_cp__c
    FROM "datawarehouse".reverse_etl."patient_to_contact_new_editions"
),
--IDS TO INSERT
ids_to_insert_table as (
    SELECT cast (cast (gp_patient_id_cp__c as float) as int) as id, gp_email__c
    FROM "datawarehouse".reverse_etl."patient_to_contact_new_additions"
),
--BAD EMAILS
failed_ids_bad_email as (
    SELECT
        gp_patient_id_cp__c as patient_id_cp,
        id as salesforce_contact_id,
        'UPDATE' as "bulk_action",
        'BAD_EMAIL' as "error_type"
    FROM ids_to_update_table
    WHERE gp_email__c !~ '^[A-Za-z0-9._%-]+@[A-Za-z0-9.-]+[.][A-Za-z]+$'
    UNION ALL
    SELECT
        id as patient_id_cp,
        NULL as salesforce_contact_id,
        'INSERT' as "bulk_action",
        'BAD_EMAIL' as "error_type"
    FROM ids_to_insert_table
    WHERE gp_email__c !~ '^[A-Za-z0-9._%-]+@[A-Za-z0-9.-]+[.][A-Za-z]+$'
),
--FAILED EDITIONS
failed_ids_to_update_table as (
    SELECT
        c.contact_gp_patient_id_cp__c as patient_id_cp,
        c.contact_id as salesforce_contact_id,
        'UPDATE' as "bulk_action",
        NULL as "error_type"
    FROM "datawarehouse".salesforce."patients_contact" as c
    WHERE
    c.contact_last_modified_date < now() - INTERVAL '10' MINUTE -- NON UPDATED ONES
    AND c.contact_id IN (SELECT id FROM ids_to_update_table)
    and c.contact_isdeleted = false
),
--FAILED INSERTIONS
success_ids_to_insert_table as (
    SELECT psf.contact_gp_patient_id_cp__c as id
    FROM "datawarehouse".salesforce."patients_contact" as psf
    WHERE
    psf.contact_gp_patient_id_cp__c IN (SELECT id FROM ids_to_insert_table)
    and psf.contact_isdeleted = false
),

failed_ids_to_insert_table as (
    SELECT
    	id as patient_id_cp,
        NULL as salesforce_contact_id,
        'INSERT' as "bulk_action",
        NULL as "error_type"
    FROM ids_to_insert_table
    WHERE id NOT IN (SELECT * FROM success_ids_to_insert_table)
),
--FINAL RESULTS
final as (
	SELECT *
	FROM failed_ids_to_update_table
	UNION
	SELECT *
	FROM failed_ids_to_insert_table
	UNION
	SELECT *
	FROM failed_ids_bad_email)
SELECT
    patient_id_cp, salesforce_contact_id, bulk_action, STRING_AGG(error_type, ', ') AS error_type,
    'now()' as execution_date,
    '2024-05-09 16:22:18.806294+00:00'::timestamp as batch_timestamp,
    FALSE as its_fixed
FROM final
GROUP BY patient_id_cp, salesforce_contact_id, bulk_action, execution_date, batch_timestamp
ORDER BY patient_id_cp, salesforce_contact_id, bulk_action, execution_date, batch_timestamp