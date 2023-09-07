SELECT d."Id" as contact_id, now() as timestamp
FROM "datawarehouse".reverse_etl."patient_to_contact_deletions" d
LEFT JOIN "datawarehouse".salesforce."patients_contact" pc ON d."Id" = pc.contact_id
WHERE pc.contact_isdeleted = true -- its deleted from Salesforce

-- Salesforce ingestion has a soft delete feature, can find the deleted Contact there, using the contact_id.