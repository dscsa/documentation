select "WhoId" as "Id", 'DUMMY' notif_type, now() as timestamp
from "datawarehouse".reverse_etl."patient_to_contact_dummies"
UNION
select "WhoId" as "Id", 'NON MERGED' notif_type, now() as timestamp
from "datawarehouse".reverse_etl."patient_to_contact_deletions_non_merged"