with _invoice_numbers as (
	select
		distinct task_subject, task_id
	from "datawarehouse".salesforce."patients_task"
	where task_contact_id is null
	and to_char(task_created_date, 'YYYY-MM-dd')  > '2023-03-27'
),
invoice_numbers as (
    SELECT cast(substring(task_subject from 'Auto .*: Good Pill is starting to prepare .* Order #(\d+)') as int) as invoice_number
        , task_id
    FROM _invoice_numbers
    UNION ALL
    SELECT cast(substring(task_subject from 'Auto .*: Good Pill received Order #(\d+) but is waiting for your prescriptions') as int) as invoice_number
        , task_id
    FROM _invoice_numbers
    UNION ALL
    SELECT cast(substring(task_subject from 'Auto .*: Good Pill update for Order #(\d+)') as int) as invoice_number
        , task_id
    FROM _invoice_numbers
    UNION ALL
    SELECT cast(substring(task_subject from 'Auto .*: Good Pill received Order #(\d+) but is waiting for your prescriptions') as int) as invoice_number
        , task_id
    FROM _invoice_numbers
    UNION ALL
    SELECT cast(substring(task_subject from 'Review Deletion: Order #(\d+)') as int) as invoice_number
        , task_id
    FROM _invoice_numbers
),
tasks_x_orders as (
	select task_id, patient_id_cp
	from "datawarehouse".goodpill."orders" o
	inner join invoice_numbers on o.invoice_number = invoice_numbers.invoice_number
),
tasks_x_orders_x_contacts as (
	select task_id as "Id", contact_id as "WhoId"
	from "datawarehouse".salesforce."patients_contact" sfpc
	inner join tasks_x_orders on sfpc.contact_gp_patient_id_cp__c = tasks_x_orders.patient_id_cp
	where contact_gp_patient_id_cp__c is not null

)
select
    *
    , '2023-11-24 12:45:40.956325+00:00'::timestamp as batch_timestamp
    , 'now()' as execution_date
from tasks_x_orders_x_contacts