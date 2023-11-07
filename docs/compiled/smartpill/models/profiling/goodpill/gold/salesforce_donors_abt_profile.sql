
    with source_data as (
      select
        *
      from "datawarehouse".salesforce."salesforce_donors_abt"
      
    ),

    column_profiles as (
      
        
        select 
          lower('task_id') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "task_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "task_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "task_id") as distinct_count,
          count(distinct "task_id") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          1 as _column_position
        from source_data

        union all
      
        
        select 
          lower('task_user_id') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "task_user_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "task_user_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "task_user_id") as distinct_count,
          count(distinct "task_user_id") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          2 as _column_position
        from source_data

        union all
      
        
        select 
          lower('task_contact_id') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "task_contact_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "task_contact_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "task_contact_id") as distinct_count,
          count(distinct "task_contact_id") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          3 as _column_position
        from source_data

        union all
      
        
        select 
          lower('task_what_id') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "task_what_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "task_what_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "task_what_id") as distinct_count,
          count(distinct "task_what_id") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          4 as _column_position
        from source_data

        union all
      
        
        select 
          lower('task_assigned_to__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "task_assigned_to__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "task_assigned_to__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "task_assigned_to__c") as distinct_count,
          count(distinct "task_assigned_to__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          5 as _column_position
        from source_data

        union all
      
        
        select 
          lower('task_subject') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "task_subject" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "task_subject") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "task_subject") as distinct_count,
          count(distinct "task_subject") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          6 as _column_position
        from source_data

        union all
      
        
        select 
          lower('task_priority') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "task_priority" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "task_priority") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "task_priority") as distinct_count,
          count(distinct "task_priority") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          7 as _column_position
        from source_data

        union all
      
        
        select 
          lower('task_type') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "task_type" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "task_type") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "task_type") as distinct_count,
          count(distinct "task_type") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          8 as _column_position
        from source_data

        union all
      
        
        select 
          lower('task_created_date') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "task_created_date" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "task_created_date") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "task_created_date") as distinct_count,
          count(distinct "task_created_date") = count(*) as is_unique,
          cast(min("task_created_date") as varchar) as min,
          cast(max("task_created_date") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          9 as _column_position
        from source_data

        union all
      
        
        select 
          lower('task_completed_datetime') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "task_completed_datetime" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "task_completed_datetime") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "task_completed_datetime") as distinct_count,
          count(distinct "task_completed_datetime") = count(*) as is_unique,
          cast(min("task_completed_datetime") as varchar) as min,
          cast(max("task_completed_datetime") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          10 as _column_position
        from source_data

        union all
      
        
        select 
          lower('task_due_date') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "task_due_date" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "task_due_date") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "task_due_date") as distinct_count,
          count(distinct "task_due_date") = count(*) as is_unique,
          cast(min("task_due_date") as varchar) as min,
          cast(max("task_due_date") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          11 as _column_position
        from source_data

        union all
      
        
        select 
          lower('call_duration_in_seconds') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "call_duration_in_seconds" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "call_duration_in_seconds") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "call_duration_in_seconds") as distinct_count,
          count(distinct "call_duration_in_seconds") = count(*) as is_unique,
          cast(min("call_duration_in_seconds") as varchar) as min,
          cast(max("call_duration_in_seconds") as varchar) as max,
          avg("call_duration_in_seconds") as avg,
          stddev_pop("call_duration_in_seconds") as std_dev_population,
          stddev_samp("call_duration_in_seconds") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          12 as _column_position
        from source_data

        union all
      
        
        select 
          lower('task_touch_reason__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "task_touch_reason__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "task_touch_reason__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "task_touch_reason__c") as distinct_count,
          count(distinct "task_touch_reason__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          13 as _column_position
        from source_data

        union all
      
        
        select 
          lower('task_status') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "task_status" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "task_status") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "task_status") as distinct_count,
          count(distinct "task_status") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          14 as _column_position
        from source_data

        union all
      
        
        select 
          lower('task_notes') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "task_notes" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "task_notes") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "task_notes") as distinct_count,
          count(distinct "task_notes") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          15 as _column_position
        from source_data

        union all
      
        
        select 
          lower('task_follow_up_notes__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "task_follow_up_notes__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "task_follow_up_notes__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "task_follow_up_notes__c") as distinct_count,
          count(distinct "task_follow_up_notes__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          16 as _column_position
        from source_data

        union all
      
        
        select 
          lower('task_follow_up_2__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "task_follow_up_2__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "task_follow_up_2__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "task_follow_up_2__c") as distinct_count,
          count(distinct "task_follow_up_2__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          17 as _column_position
        from source_data

        union all
      
        
        select 
          lower('task_description') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "task_description" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "task_description") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "task_description") as distinct_count,
          count(distinct "task_description") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          18 as _column_position
        from source_data

        union all
      
        
        select 
          lower('task_last_modified_date') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "task_last_modified_date" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "task_last_modified_date") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "task_last_modified_date") as distinct_count,
          count(distinct "task_last_modified_date") = count(*) as is_unique,
          cast(min("task_last_modified_date") as varchar) as min,
          cast(max("task_last_modified_date") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          19 as _column_position
        from source_data

        union all
      
        
        select 
          lower('task_is_deleted') as column_name,
          nullif(lower('boolean'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "task_is_deleted" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "task_is_deleted") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "task_is_deleted") as distinct_count,
          count(distinct "task_is_deleted") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          20 as _column_position
        from source_data

        union all
      
        
        select 
          lower('task_airbyte_emitted_at') as column_name,
          nullif(lower('timestamp with time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "task_airbyte_emitted_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "task_airbyte_emitted_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "task_airbyte_emitted_at") as distinct_count,
          count(distinct "task_airbyte_emitted_at") = count(*) as is_unique,
          cast(min("task_airbyte_emitted_at") as varchar) as min,
          cast(max("task_airbyte_emitted_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          21 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_id') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_id") as distinct_count,
          count(distinct "contact_id") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          22 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_owner_id') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_owner_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_owner_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_owner_id") as distinct_count,
          count(distinct "contact_owner_id") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          23 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_name') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_name" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_name") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_name") as distinct_count,
          count(distinct "contact_name") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          24 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_birthdate') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_birthdate" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_birthdate") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_birthdate") as distinct_count,
          count(distinct "contact_birthdate") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          25 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_lastname') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_lastname" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_lastname") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_lastname") as distinct_count,
          count(distinct "contact_lastname") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          26 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_firstname') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_firstname" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_firstname") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_firstname") as distinct_count,
          count(distinct "contact_firstname") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          27 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_email') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_email" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_email") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_email") as distinct_count,
          count(distinct "contact_email") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          28 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_phone') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_phone" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_phone") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_phone") as distinct_count,
          count(distinct "contact_phone") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          29 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_title') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_title" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_title") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_title") as distinct_count,
          count(distinct "contact_title") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          30 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_description') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_description" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_description") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_description") as distinct_count,
          count(distinct "contact_description") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          31 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_last_modified_date') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_last_modified_date" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_last_modified_date") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_last_modified_date") as distinct_count,
          count(distinct "contact_last_modified_date") = count(*) as is_unique,
          cast(min("contact_last_modified_date") as varchar) as min,
          cast(max("contact_last_modified_date") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          32 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_isdeleted') as column_name,
          nullif(lower('boolean'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_isdeleted" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_isdeleted") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_isdeleted") as distinct_count,
          count(distinct "contact_isdeleted") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          33 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_airbyte_emitted_at') as column_name,
          nullif(lower('timestamp with time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_airbyte_emitted_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_airbyte_emitted_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_airbyte_emitted_at") as distinct_count,
          count(distinct "contact_airbyte_emitted_at") = count(*) as is_unique,
          cast(min("contact_airbyte_emitted_at") as varchar) as min,
          cast(max("contact_airbyte_emitted_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          34 as _column_position
        from source_data

        union all
      
        
        select 
          lower('gp_user_id') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "gp_user_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "gp_user_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "gp_user_id") as distinct_count,
          count(distinct "gp_user_id") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          35 as _column_position
        from source_data

        union all
      
        
        select 
          lower('gp_user_ownerid') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "gp_user_ownerid" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "gp_user_ownerid") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "gp_user_ownerid") as distinct_count,
          count(distinct "gp_user_ownerid") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          36 as _column_position
        from source_data

        union all
      
        
        select 
          lower('gp_user_name') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "gp_user_name" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "gp_user_name") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "gp_user_name") as distinct_count,
          count(distinct "gp_user_name") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          37 as _column_position
        from source_data

        union all
      
        
        select 
          lower('gp_user_role__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "gp_user_role__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "gp_user_role__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "gp_user_role__c") as distinct_count,
          count(distinct "gp_user_role__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          38 as _column_position
        from source_data

        union all
      
        
        select 
          lower('gp_user_email__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "gp_user_email__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "gp_user_email__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "gp_user_email__c") as distinct_count,
          count(distinct "gp_user_email__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          39 as _column_position
        from source_data

        union all
      
        
        select 
          lower('gp_user_airbyte_emitted_at') as column_name,
          nullif(lower('timestamp with time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "gp_user_airbyte_emitted_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "gp_user_airbyte_emitted_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "gp_user_airbyte_emitted_at") as distinct_count,
          count(distinct "gp_user_airbyte_emitted_at") = count(*) as is_unique,
          cast(min("gp_user_airbyte_emitted_at") as varchar) as min,
          cast(max("gp_user_airbyte_emitted_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          40 as _column_position
        from source_data

        union all
      
        
        select 
          lower('gp_user_isdeleted') as column_name,
          nullif(lower('boolean'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "gp_user_isdeleted" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "gp_user_isdeleted") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "gp_user_isdeleted") as distinct_count,
          count(distinct "gp_user_isdeleted") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          41 as _column_position
        from source_data

        union all
      
        
        select 
          lower('_airbyte_emitted_at') as column_name,
          nullif(lower('timestamp with time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "_airbyte_emitted_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "_airbyte_emitted_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "_airbyte_emitted_at") as distinct_count,
          count(distinct "_airbyte_emitted_at") = count(*) as is_unique,
          cast(min("_airbyte_emitted_at") as varchar) as min,
          cast(max("_airbyte_emitted_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          42 as _column_position
        from source_data

        
      
    )

    select
      column_name,
      data_type,
      
        row_count,
      
        not_null_proportion,
      
        distinct_proportion,
      
        distinct_count,
      
        is_unique,
      
        min,
      
        max,
      
        avg,
      
        std_dev_population,
      
        std_dev_sample,
      
      profiled_at
    from column_profiles
    order by _column_position asc
  