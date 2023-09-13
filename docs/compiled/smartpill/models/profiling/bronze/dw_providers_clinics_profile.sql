
    with source_data as (
      select
        *
      from "datawarehouse".goodpill."dw_providers_clinics"
      
    ),

    column_profiles as (
      
        
        select 
          lower('provider_clinic_id') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "provider_clinic_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "provider_clinic_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "provider_clinic_id") as distinct_count,
          count(distinct "provider_clinic_id") = count(*) as is_unique,
          cast(min("provider_clinic_id") as varchar) as min,
          cast(max("provider_clinic_id") as varchar) as max,
          avg("provider_clinic_id") as avg,
          stddev_pop("provider_clinic_id") as std_dev_population,
          stddev_samp("provider_clinic_id") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          1 as _column_position
        from source_data

        union all
      
        
        select 
          lower('clinic_id') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "clinic_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "clinic_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "clinic_id") as distinct_count,
          count(distinct "clinic_id") = count(*) as is_unique,
          cast(min("clinic_id") as varchar) as min,
          cast(max("clinic_id") as varchar) as max,
          avg("clinic_id") as avg,
          stddev_pop("clinic_id") as std_dev_population,
          stddev_samp("clinic_id") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          2 as _column_position
        from source_data

        union all
      
        
        select 
          lower('provider_id') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "provider_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "provider_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "provider_id") as distinct_count,
          count(distinct "provider_id") = count(*) as is_unique,
          cast(min("provider_id") as varchar) as min,
          cast(max("provider_id") as varchar) as max,
          avg("provider_id") as avg,
          stddev_pop("provider_id") as std_dev_population,
          stddev_samp("provider_id") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          3 as _column_position
        from source_data

        union all
      
        
        select 
          lower('started_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "started_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "started_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "started_at") as distinct_count,
          count(distinct "started_at") = count(*) as is_unique,
          cast(min("started_at") as varchar) as min,
          cast(max("started_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          4 as _column_position
        from source_data

        union all
      
        
        select 
          lower('stopped_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "stopped_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "stopped_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "stopped_at") as distinct_count,
          count(distinct "stopped_at") = count(*) as is_unique,
          cast(min("stopped_at") as varchar) as min,
          cast(max("stopped_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          5 as _column_position
        from source_data

        union all
      
        
        select 
          lower('created_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "created_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "created_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "created_at") as distinct_count,
          count(distinct "created_at") = count(*) as is_unique,
          cast(min("created_at") as varchar) as min,
          cast(max("created_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          6 as _column_position
        from source_data

        union all
      
        
        select 
          lower('updated_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "updated_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "updated_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "updated_at") as distinct_count,
          count(distinct "updated_at") = count(*) as is_unique,
          cast(min("updated_at") as varchar) as min,
          cast(max("updated_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          7 as _column_position
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
  