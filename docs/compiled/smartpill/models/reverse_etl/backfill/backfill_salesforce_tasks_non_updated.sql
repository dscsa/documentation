WITH
ids_to_update_table as (
    SELECT *
    FROM "datawarehouse".prod."backfill_salesforce_tasks"
    WHERE
        execution_date = 'now()'
        AND batch_timestamp = (
            SELECT MAX(batch_timestamp)
            FROM "datawarehouse".prod."backfill_salesforce_tasks"
            WHERE execution_date = 'now()'
        )
),
failed_ids_to_update_table as (
    SELECT ids_to_update_table.*
    FROM ids_to_update_table
    LEFT JOIN "datawarehouse".salesforce."patients_task" as updated_pt ON (ids_to_update_table."Id" = updated_pt.task_id)
    WHERE updated_pt.task_contact_id is null
)
select
*
, 'UPDATE' as "bulk_action"
, 'BACKFILL_ERROR' as "error_type"
from failed_ids_to_update_table