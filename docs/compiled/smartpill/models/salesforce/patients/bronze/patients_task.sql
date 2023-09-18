-- in case of full-refresh need to filter incremental rows from raw source!
with _task as (
	select *, ROW_NUMBER() OVER(
		partition by jsonb_extract_path_text(_airbyte_data, 'Id')
		order by "_airbyte_emitted_at" desc
	) as id_row_number
	from "datawarehouse"."raw"._airbyte_raw_salesforce_task
    
    where _airbyte_emitted_at > (
        select max(task_airbyte_emitted_at) from "datawarehouse".salesforce."patients_task"
    )
    
),
task as (
    select
    --ids
    jsonb_extract_path_text(_airbyte_data, 'Id') as task_id,
    jsonb_extract_path_text(_airbyte_data, 'OwnerId') as task_user_id, --refers to user
    jsonb_extract_path_text(_airbyte_data, 'WhoId') as task_contact_id, --refers to contact, lead
    jsonb_extract_path_text(_airbyte_data, 'WhatId') as task_what_id,
    jsonb_extract_path_text(_airbyte_data, 'Related_to_ID__c') as task_related_to_id__c,
    jsonb_extract_path_text(_airbyte_data, 'Assigned_To__c') as task_assigned_to__c,
    jsonb_extract_path_text(_airbyte_data, 'AccountId') as task_account_id,
    --data
    jsonb_extract_path_text(_airbyte_data, 'Subject') as task_subject,
    jsonb_extract_path_text(_airbyte_data, 'Priority') as task_priority,
    coalesce (
        jsonb_extract_path_text(_airbyte_data, 'Types__c'),
        jsonb_extract_path_text(_airbyte_data, 'Type')
    ) as task_type,
    cast(jsonb_extract_path_text(_airbyte_data, 'CreatedDate') as timestamp) as task_created_date,
    cast(jsonb_extract_path_text(_airbyte_data, 'LastModifiedDate') as timestamp) as task_last_modified_date,

    (case when
        jsonb_extract_path_text(_airbyte_data, 'Status') = 'Completed'
        and jsonb_extract_path_text(_airbyte_data, 'CompletedDateTime') is null
    then cast(jsonb_extract_path_text(_airbyte_data, 'CreatedDate') as timestamp)
    else cast(jsonb_extract_path_text(_airbyte_data, 'CompletedDateTime') as timestamp)
    end) as task_completed_datetime,

    cast(jsonb_extract_path_text(_airbyte_data, 'ActivityDate') as timestamp) as task_due_date,
    cast(jsonb_extract_path_text(_airbyte_data, 'CallDurationInSeconds') as integer) as call_duration_in_seconds,

    jsonb_extract_path_text(_airbyte_data, 'aircall__Country__c') as aircall__country__c,
    jsonb_extract_path_text(_airbyte_data, 'aircall__Timezone__c') as aircall__timezone__c,
    jsonb_extract_path_text(_airbyte_data, 'aircall__Answered_by__c') as aircall__answered_by__c,
    jsonb_extract_path_text(_airbyte_data, 'aircall__Is_Voicemail__c') as aircall__is_voicemail__c,
    jsonb_extract_path_text(_airbyte_data, 'aircall__Phone_number__c') as aircall__phone_number__c,
    cast(jsonb_extract_path_text(_airbyte_data, 'aircall__Waiting_Time__c') as decimal(10,3)) as aircall__waiting_time__c,
    jsonb_extract_path_text(_airbyte_data, 'aircall__Detailed_type__c') as aircall__detailed_type__c,
    jsonb_extract_path_text(_airbyte_data, 'aircall__Has_connected__c') as aircall__has_connected__c,
    jsonb_extract_path_text(_airbyte_data, 'aircall__Call_Recording__c') as aircall__call_recording__c,
    jsonb_extract_path_text(_airbyte_data, 'aircall__Is_Missed_call__c') as aircall__is_missed_call__c,
    jsonb_extract_path_text(_airbyte_data, 'aircall__Transferred_to__c') as aircall__transferred_to__c,
    jsonb_extract_path_text(_airbyte_data, 'aircall__Hour_of_the_day__c') as aircall__hour_of_the_day__c,
    jsonb_extract_path_text(_airbyte_data, 'aircall__Connection_status__c') as aircall__connection_status__c,
    jsonb_extract_path_text(_airbyte_data, 'aircall__Missed_Call_Ratio__c') as aircall__missed_call_ratio__c,

    jsonb_extract_path_text(_airbyte_data, 'Status') as task_status,
    jsonb_extract_path_text(_airbyte_data, 'Notes__c') as task_notes,
    jsonb_extract_path_text(_airbyte_data, 'Follow_Up_Notes__c') as task_follow_up_notes__c,
    jsonb_extract_path_text(_airbyte_data, 'Follow_Up_2__c') as task_follow_up_2__c,
    jsonb_extract_path_text(_airbyte_data, 'Description') as task_description,

    jsonb_extract_path_text(_airbyte_data, 'IsDeleted')::bool as task_is_deleted,
    _airbyte_emitted_at as task_airbyte_emitted_at

    from _task where id_row_number = 1
)
select * from task