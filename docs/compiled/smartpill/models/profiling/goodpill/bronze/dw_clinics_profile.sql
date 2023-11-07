
    with source_data as (
      select
        *
      from "datawarehouse".goodpill."dw_clinics"
      
    ),

    column_profiles as (
      
        
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
          1 as _column_position
        from source_data

        union all
      
        
        select 
          lower('clinic_group_id') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "clinic_group_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "clinic_group_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "clinic_group_id") as distinct_count,
          count(distinct "clinic_group_id") = count(*) as is_unique,
          cast(min("clinic_group_id") as varchar) as min,
          cast(max("clinic_group_id") as varchar) as max,
          avg("clinic_group_id") as avg,
          stddev_pop("clinic_group_id") as std_dev_population,
          stddev_samp("clinic_group_id") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          2 as _column_position
        from source_data

        union all
      
        
        select 
          lower('clinic_name') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "clinic_name" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "clinic_name") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "clinic_name") as distinct_count,
          count(distinct "clinic_name") = count(*) as is_unique,
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
          lower('clinic_name_cp') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "clinic_name_cp" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "clinic_name_cp") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "clinic_name_cp") as distinct_count,
          count(distinct "clinic_name_cp") = count(*) as is_unique,
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
          lower('clinic_address') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "clinic_address" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "clinic_address") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "clinic_address") as distinct_count,
          count(distinct "clinic_address") = count(*) as is_unique,
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
          lower('clinic_street') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "clinic_street" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "clinic_street") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "clinic_street") as distinct_count,
          count(distinct "clinic_street") = count(*) as is_unique,
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
          lower('clinic_city') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "clinic_city" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "clinic_city") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "clinic_city") as distinct_count,
          count(distinct "clinic_city") = count(*) as is_unique,
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
          lower('clinic_state') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "clinic_state" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "clinic_state") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "clinic_state") as distinct_count,
          count(distinct "clinic_state") = count(*) as is_unique,
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
          lower('clinic_zip') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "clinic_zip" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "clinic_zip") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "clinic_zip") as distinct_count,
          count(distinct "clinic_zip") = count(*) as is_unique,
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
          lower('clinic_phone') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "clinic_phone" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "clinic_phone") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "clinic_phone") as distinct_count,
          count(distinct "clinic_phone") = count(*) as is_unique,
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
          lower('clinic_id_sf') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "clinic_id_sf" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "clinic_id_sf") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "clinic_id_sf") as distinct_count,
          count(distinct "clinic_id_sf") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          11 as _column_position
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
          12 as _column_position
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
          13 as _column_position
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
  