
    with source_data as (
      select
        *
      from "datawarehouse".salesforce."salesforce_patients_abt"
      
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
          lower('task_related_to_id__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "task_related_to_id__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "task_related_to_id__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "task_related_to_id__c") as distinct_count,
          count(distinct "task_related_to_id__c") = count(*) as is_unique,
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
          6 as _column_position
        from source_data

        union all
      
        
        select 
          lower('task_account_id') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "task_account_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "task_account_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "task_account_id") as distinct_count,
          count(distinct "task_account_id") = count(*) as is_unique,
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
          8 as _column_position
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
          9 as _column_position
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
          10 as _column_position
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
          11 as _column_position
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
          12 as _column_position
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
          13 as _column_position
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
          14 as _column_position
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
          15 as _column_position
        from source_data

        union all
      
        
        select 
          lower('aircall__country__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "aircall__country__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "aircall__country__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "aircall__country__c") as distinct_count,
          count(distinct "aircall__country__c") = count(*) as is_unique,
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
          lower('aircall__timezone__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "aircall__timezone__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "aircall__timezone__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "aircall__timezone__c") as distinct_count,
          count(distinct "aircall__timezone__c") = count(*) as is_unique,
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
          lower('aircall__answered_by__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "aircall__answered_by__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "aircall__answered_by__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "aircall__answered_by__c") as distinct_count,
          count(distinct "aircall__answered_by__c") = count(*) as is_unique,
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
          lower('aircall__is_voicemail__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "aircall__is_voicemail__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "aircall__is_voicemail__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "aircall__is_voicemail__c") as distinct_count,
          count(distinct "aircall__is_voicemail__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          19 as _column_position
        from source_data

        union all
      
        
        select 
          lower('aircall__phone_number__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "aircall__phone_number__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "aircall__phone_number__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "aircall__phone_number__c") as distinct_count,
          count(distinct "aircall__phone_number__c") = count(*) as is_unique,
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
          lower('aircall__waiting_time__c') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "aircall__waiting_time__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "aircall__waiting_time__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "aircall__waiting_time__c") as distinct_count,
          count(distinct "aircall__waiting_time__c") = count(*) as is_unique,
          cast(min("aircall__waiting_time__c") as varchar) as min,
          cast(max("aircall__waiting_time__c") as varchar) as max,
          avg("aircall__waiting_time__c") as avg,
          stddev_pop("aircall__waiting_time__c") as std_dev_population,
          stddev_samp("aircall__waiting_time__c") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          21 as _column_position
        from source_data

        union all
      
        
        select 
          lower('aircall__detailed_type__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "aircall__detailed_type__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "aircall__detailed_type__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "aircall__detailed_type__c") as distinct_count,
          count(distinct "aircall__detailed_type__c") = count(*) as is_unique,
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
          lower('aircall__has_connected__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "aircall__has_connected__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "aircall__has_connected__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "aircall__has_connected__c") as distinct_count,
          count(distinct "aircall__has_connected__c") = count(*) as is_unique,
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
          lower('aircall__call_recording__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "aircall__call_recording__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "aircall__call_recording__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "aircall__call_recording__c") as distinct_count,
          count(distinct "aircall__call_recording__c") = count(*) as is_unique,
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
          lower('aircall__is_missed_call__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "aircall__is_missed_call__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "aircall__is_missed_call__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "aircall__is_missed_call__c") as distinct_count,
          count(distinct "aircall__is_missed_call__c") = count(*) as is_unique,
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
          lower('aircall__transferred_to__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "aircall__transferred_to__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "aircall__transferred_to__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "aircall__transferred_to__c") as distinct_count,
          count(distinct "aircall__transferred_to__c") = count(*) as is_unique,
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
          lower('aircall__hour_of_the_day__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "aircall__hour_of_the_day__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "aircall__hour_of_the_day__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "aircall__hour_of_the_day__c") as distinct_count,
          count(distinct "aircall__hour_of_the_day__c") = count(*) as is_unique,
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
          lower('aircall__connection_status__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "aircall__connection_status__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "aircall__connection_status__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "aircall__connection_status__c") as distinct_count,
          count(distinct "aircall__connection_status__c") = count(*) as is_unique,
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
          lower('aircall__missed_call_ratio__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "aircall__missed_call_ratio__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "aircall__missed_call_ratio__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "aircall__missed_call_ratio__c") as distinct_count,
          count(distinct "aircall__missed_call_ratio__c") = count(*) as is_unique,
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
          30 as _column_position
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
          31 as _column_position
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
          32 as _column_position
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
          33 as _column_position
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
          34 as _column_position
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
          35 as _column_position
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
          36 as _column_position
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
          37 as _column_position
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
          38 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_gp_patient_id_cp__c') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_gp_patient_id_cp__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_gp_patient_id_cp__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_gp_patient_id_cp__c") as distinct_count,
          count(distinct "contact_gp_patient_id_cp__c") = count(*) as is_unique,
          cast(min("contact_gp_patient_id_cp__c") as varchar) as min,
          cast(max("contact_gp_patient_id_cp__c") as varchar) as max,
          avg("contact_gp_patient_id_cp__c") as avg,
          stddev_pop("contact_gp_patient_id_cp__c") as std_dev_population,
          stddev_samp("contact_gp_patient_id_cp__c") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          39 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_gp_patient_id_wc__c') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_gp_patient_id_wc__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_gp_patient_id_wc__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_gp_patient_id_wc__c") as distinct_count,
          count(distinct "contact_gp_patient_id_wc__c") = count(*) as is_unique,
          cast(min("contact_gp_patient_id_wc__c") as varchar) as min,
          cast(max("contact_gp_patient_id_wc__c") as varchar) as max,
          avg("contact_gp_patient_id_wc__c") as avg,
          stddev_pop("contact_gp_patient_id_wc__c") as std_dev_population,
          stddev_samp("contact_gp_patient_id_wc__c") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          40 as _column_position
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
          41 as _column_position
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
          42 as _column_position
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
          43 as _column_position
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
          44 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_created_date') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_created_date" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_created_date") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_created_date") as distinct_count,
          count(distinct "contact_created_date") = count(*) as is_unique,
          cast(min("contact_created_date") as varchar) as min,
          cast(max("contact_created_date") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          45 as _column_position
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
          46 as _column_position
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
          47 as _column_position
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
          48 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_gp_payment_card_type__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_gp_payment_card_type__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_gp_payment_card_type__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_gp_payment_card_type__c") as distinct_count,
          count(distinct "contact_gp_payment_card_type__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          49 as _column_position
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
          50 as _column_position
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
          51 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_account_id') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_account_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_account_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_account_id") as distinct_count,
          count(distinct "contact_account_id") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          52 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_gp_language__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_gp_language__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_gp_language__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_gp_language__c") as distinct_count,
          count(distinct "contact_gp_language__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          53 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_gp_patient_address1__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_gp_patient_address1__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_gp_patient_address1__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_gp_patient_address1__c") as distinct_count,
          count(distinct "contact_gp_patient_address1__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          54 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_gp_patient_address2__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_gp_patient_address2__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_gp_patient_address2__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_gp_patient_address2__c") as distinct_count,
          count(distinct "contact_gp_patient_address2__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          55 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_gp_patient_autofill__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_gp_patient_autofill__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_gp_patient_autofill__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_gp_patient_autofill__c") as distinct_count,
          count(distinct "contact_gp_patient_autofill__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          56 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_gp_patient_city__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_gp_patient_city__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_gp_patient_city__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_gp_patient_city__c") as distinct_count,
          count(distinct "contact_gp_patient_city__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          57 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_gp_patient_date_added__c') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_gp_patient_date_added__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_gp_patient_date_added__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_gp_patient_date_added__c") as distinct_count,
          count(distinct "contact_gp_patient_date_added__c") = count(*) as is_unique,
          cast(min("contact_gp_patient_date_added__c") as varchar) as min,
          cast(max("contact_gp_patient_date_added__c") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          58 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_gp_patient_date_changed__c') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_gp_patient_date_changed__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_gp_patient_date_changed__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_gp_patient_date_changed__c") as distinct_count,
          count(distinct "contact_gp_patient_date_changed__c") = count(*) as is_unique,
          cast(min("contact_gp_patient_date_changed__c") as varchar) as min,
          cast(max("contact_gp_patient_date_changed__c") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          59 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_gp_patient_note__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_gp_patient_note__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_gp_patient_note__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_gp_patient_note__c") as distinct_count,
          count(distinct "contact_gp_patient_note__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          60 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_gp_patient_state__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_gp_patient_state__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_gp_patient_state__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_gp_patient_state__c") as distinct_count,
          count(distinct "contact_gp_patient_state__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          61 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_gp_patient_status__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_gp_patient_status__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_gp_patient_status__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_gp_patient_status__c") as distinct_count,
          count(distinct "contact_gp_patient_status__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          62 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_gp_patient_zip__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_gp_patient_zip__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_gp_patient_zip__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_gp_patient_zip__c") as distinct_count,
          count(distinct "contact_gp_patient_zip__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          63 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_gp_payment_card_date_expired__c') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_gp_payment_card_date_expired__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_gp_payment_card_date_expired__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_gp_payment_card_date_expired__c") as distinct_count,
          count(distinct "contact_gp_payment_card_date_expired__c") = count(*) as is_unique,
          cast(min("contact_gp_payment_card_date_expired__c") as varchar) as min,
          cast(max("contact_gp_payment_card_date_expired__c") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          64 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_gp_payment_card_last4__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_gp_payment_card_last4__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_gp_payment_card_last4__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_gp_payment_card_last4__c") as distinct_count,
          count(distinct "contact_gp_payment_card_last4__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          65 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_gp_payment_coupon__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_gp_payment_coupon__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_gp_payment_coupon__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_gp_payment_coupon__c") as distinct_count,
          count(distinct "contact_gp_payment_coupon__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          66 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_gp_payment_method__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_gp_payment_method__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_gp_payment_method__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_gp_payment_method__c") as distinct_count,
          count(distinct "contact_gp_payment_method__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          67 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_gp_pharmacy_fax__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_gp_pharmacy_fax__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_gp_pharmacy_fax__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_gp_pharmacy_fax__c") as distinct_count,
          count(distinct "contact_gp_pharmacy_fax__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          68 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_gp_pharmacy_npi__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_gp_pharmacy_npi__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_gp_pharmacy_npi__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_gp_pharmacy_npi__c") as distinct_count,
          count(distinct "contact_gp_pharmacy_npi__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          69 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_gp_pharmacy_name__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_gp_pharmacy_name__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_gp_pharmacy_name__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_gp_pharmacy_name__c") as distinct_count,
          count(distinct "contact_gp_pharmacy_name__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          70 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_gp_pharmacy_phone__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_gp_pharmacy_phone__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_gp_pharmacy_phone__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_gp_pharmacy_phone__c") as distinct_count,
          count(distinct "contact_gp_pharmacy_phone__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          71 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_gp_pharmacy_address__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_gp_pharmacy_address__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_gp_pharmacy_address__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_gp_pharmacy_address__c") as distinct_count,
          count(distinct "contact_gp_pharmacy_address__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          72 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_gp_phone1__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_gp_phone1__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_gp_phone1__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_gp_phone1__c") as distinct_count,
          count(distinct "contact_gp_phone1__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          73 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_gp_phone2__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_gp_phone2__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_gp_phone2__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_gp_phone2__c") as distinct_count,
          count(distinct "contact_gp_phone2__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          74 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_gp_refills_used__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_gp_refills_used__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_gp_refills_used__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_gp_refills_used__c") as distinct_count,
          count(distinct "contact_gp_refills_used__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          75 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_gp_tracking_coupon__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_gp_tracking_coupon__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_gp_tracking_coupon__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_gp_tracking_coupon__c") as distinct_count,
          count(distinct "contact_gp_tracking_coupon__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          76 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_recordtype_id') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_recordtype_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_recordtype_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_recordtype_id") as distinct_count,
          count(distinct "contact_recordtype_id") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          77 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_first_name__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_first_name__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_first_name__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_first_name__c") as distinct_count,
          count(distinct "contact_first_name__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          78 as _column_position
        from source_data

        union all
      
        
        select 
          lower('contact_gp_email__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "contact_gp_email__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "contact_gp_email__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "contact_gp_email__c") as distinct_count,
          count(distinct "contact_gp_email__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          79 as _column_position
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
          80 as _column_position
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
          81 as _column_position
        from source_data

        union all
      
        
        select 
          lower('gp_user_assigned_to_id') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "gp_user_assigned_to_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "gp_user_assigned_to_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "gp_user_assigned_to_id") as distinct_count,
          count(distinct "gp_user_assigned_to_id") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          82 as _column_position
        from source_data

        union all
      
        
        select 
          lower('gp_user_assigned_to_ownerid') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "gp_user_assigned_to_ownerid" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "gp_user_assigned_to_ownerid") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "gp_user_assigned_to_ownerid") as distinct_count,
          count(distinct "gp_user_assigned_to_ownerid") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          83 as _column_position
        from source_data

        union all
      
        
        select 
          lower('gp_user_assigned_to_name') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "gp_user_assigned_to_name" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "gp_user_assigned_to_name") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "gp_user_assigned_to_name") as distinct_count,
          count(distinct "gp_user_assigned_to_name") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          84 as _column_position
        from source_data

        union all
      
        
        select 
          lower('gp_user_assigned_to_role__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "gp_user_assigned_to_role__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "gp_user_assigned_to_role__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "gp_user_assigned_to_role__c") as distinct_count,
          count(distinct "gp_user_assigned_to_role__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          85 as _column_position
        from source_data

        union all
      
        
        select 
          lower('gp_user_assigned_to_email__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "gp_user_assigned_to_email__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "gp_user_assigned_to_email__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "gp_user_assigned_to_email__c") as distinct_count,
          count(distinct "gp_user_assigned_to_email__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          86 as _column_position
        from source_data

        union all
      
        
        select 
          lower('gp_user_related_to_id') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "gp_user_related_to_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "gp_user_related_to_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "gp_user_related_to_id") as distinct_count,
          count(distinct "gp_user_related_to_id") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          87 as _column_position
        from source_data

        union all
      
        
        select 
          lower('gp_user_related_to_ownerid') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "gp_user_related_to_ownerid" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "gp_user_related_to_ownerid") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "gp_user_related_to_ownerid") as distinct_count,
          count(distinct "gp_user_related_to_ownerid") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          88 as _column_position
        from source_data

        union all
      
        
        select 
          lower('gp_user_related_to_name') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "gp_user_related_to_name" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "gp_user_related_to_name") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "gp_user_related_to_name") as distinct_count,
          count(distinct "gp_user_related_to_name") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          89 as _column_position
        from source_data

        union all
      
        
        select 
          lower('gp_user_related_to_role__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "gp_user_related_to_role__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "gp_user_related_to_role__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "gp_user_related_to_role__c") as distinct_count,
          count(distinct "gp_user_related_to_role__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          90 as _column_position
        from source_data

        union all
      
        
        select 
          lower('gp_user_related_to_email__c') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "gp_user_related_to_email__c" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "gp_user_related_to_email__c") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "gp_user_related_to_email__c") as distinct_count,
          count(distinct "gp_user_related_to_email__c") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          91 as _column_position
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
          92 as _column_position
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
  