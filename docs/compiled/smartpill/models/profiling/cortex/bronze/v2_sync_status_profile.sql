
    with source_data as (
      select
        *
      from "datawarehouse".cortex."v2_sync_status"
      
    ),

    column_profiles as (
      
        
        select 
          lower('db') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "db" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "db") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "db") as distinct_count,
          count(distinct "db") = count(*) as is_unique,
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
          lower('seq') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "seq" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "seq") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "seq") as distinct_count,
          count(distinct "seq") = count(*) as is_unique,
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
          lower('last_sync_started_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "last_sync_started_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "last_sync_started_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "last_sync_started_at") as distinct_count,
          count(distinct "last_sync_started_at") = count(*) as is_unique,
          cast(min("last_sync_started_at") as varchar) as min,
          cast(max("last_sync_started_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          3 as _column_position
        from source_data

        union all
      
        
        select 
          lower('last_sync_completed_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "last_sync_completed_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "last_sync_completed_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "last_sync_completed_at") as distinct_count,
          count(distinct "last_sync_completed_at") = count(*) as is_unique,
          cast(min("last_sync_completed_at") as varchar) as min,
          cast(max("last_sync_completed_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          4 as _column_position
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
  