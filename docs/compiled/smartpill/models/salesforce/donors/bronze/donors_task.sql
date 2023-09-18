-- in case of full-refresh need to filter incremental rows from raw source!
with _task as (
	select *,
	ROW_NUMBER() OVER(
		partition by jsonb_extract_path_text(_airbyte_data, 'Id')
		order by "_airbyte_emitted_at" desc
	) as id_row_number
	from "datawarehouse"."raw"._airbyte_raw_salesforce_donors_task
    
        where _airbyte_emitted_at > (
			select max(task_airbyte_emitted_at) from "datawarehouse".salesforce."donors_task"
		)
    
),

task as (
    select
    --ids
    jsonb_extract_path_text(_airbyte_data, 'Id') as task_id,
    jsonb_extract_path_text(_airbyte_data, 'OwnerId') as task_user_id, --refers to user
    jsonb_extract_path_text(_airbyte_data, 'WhoId') as task_contact_id, --refers to contact, lead
    jsonb_extract_path_text(_airbyte_data, 'WhatId') as task_what_id, -- to gp_user
    jsonb_extract_path_text(_airbyte_data, 'Assigned_To__c') as task_assigned_to__c, -- didnt found a join
    --data
    jsonb_extract_path_text(_airbyte_data, 'Subject') as task_subject,
    jsonb_extract_path_text(_airbyte_data, 'Priority') as task_priority,
    coalesce (
        jsonb_extract_path_text(_airbyte_data, 'Types__c'),
        jsonb_extract_path_text(_airbyte_data, 'Type')
    ) as task_type,
    cast(jsonb_extract_path_text(_airbyte_data, 'CreatedDate') as timestamp) as task_created_date,

    (case when
        jsonb_extract_path_text(_airbyte_data, 'Status') = 'Completed'
        and jsonb_extract_path_text(_airbyte_data, 'CompletedDateTime') is null
    then cast(jsonb_extract_path_text(_airbyte_data, 'CreatedDate') as timestamp)
    else cast(jsonb_extract_path_text(_airbyte_data, 'CompletedDateTime') as timestamp)
    end) as task_completed_datetime,

    cast(jsonb_extract_path_text(_airbyte_data, 'ActivityDate') as timestamp) as task_due_date,
    cast(jsonb_extract_path_text(_airbyte_data, 'CallDurationInSeconds') as integer) as call_duration_in_seconds,

    jsonb_extract_path_text(_airbyte_data, 'Touch_Reason__c') as task_touch_reason__c,
    jsonb_extract_path_text(_airbyte_data, 'Status') as task_status,
    jsonb_extract_path_text(_airbyte_data, 'Notes__c') as task_notes,
    jsonb_extract_path_text(_airbyte_data, 'Follow_Up_Notes__c') as task_follow_up_notes__c,
    jsonb_extract_path_text(_airbyte_data, 'Follow_Up_2__c') as task_follow_up_2__c,
    jsonb_extract_path_text(_airbyte_data, 'Description') as task_description,
    cast(jsonb_extract_path_text(_airbyte_data, 'LastModifiedDate') as timestamp) as task_last_modified_date,

    jsonb_extract_path_text(_airbyte_data, 'IsDeleted')::bool as task_is_deleted,
    _airbyte_emitted_at as task_airbyte_emitted_at

    from _task
    where id_row_number = 1

)
select * from task