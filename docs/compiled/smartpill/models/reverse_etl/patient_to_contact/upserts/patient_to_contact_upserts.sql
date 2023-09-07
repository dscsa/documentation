SELECT *
FROM "datawarehouse".reverse_etl."patient_to_contact_new_additions"
UNION
SELECT *
FROM "datawarehouse".reverse_etl."patient_to_contact_new_editions"