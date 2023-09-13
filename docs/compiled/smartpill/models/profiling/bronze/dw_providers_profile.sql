
    with source_data as (
      select
        *
      from "datawarehouse".goodpill."dw_providers"
      
    ),

    column_profiles as (
      
        
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
          1 as _column_position
        from source_data

        union all
      
        
        select 
          lower('provider_npi') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "provider_npi" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "provider_npi") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "provider_npi") as distinct_count,
          count(distinct "provider_npi") = count(*) as is_unique,
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
          lower('provider_name') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "provider_name" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "provider_name") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "provider_name") as distinct_count,
          count(distinct "provider_name") = count(*) as is_unique,
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
          lower('provider_phone') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "provider_phone" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "provider_phone") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "provider_phone") as distinct_count,
          count(distinct "provider_phone") = count(*) as is_unique,
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
          lower('provider_id_sf') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "provider_id_sf" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "provider_id_sf") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "provider_id_sf") as distinct_count,
          count(distinct "provider_id_sf") = count(*) as is_unique,
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
          lower('default_clinic') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "default_clinic" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "default_clinic") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "default_clinic") as distinct_count,
          count(distinct "default_clinic") = count(*) as is_unique,
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
          lower('default_clinic_imputed_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "default_clinic_imputed_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "default_clinic_imputed_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "default_clinic_imputed_at") as distinct_count,
          count(distinct "default_clinic_imputed_at") = count(*) as is_unique,
          cast(min("default_clinic_imputed_at") as varchar) as min,
          cast(max("default_clinic_imputed_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          7 as _column_position
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
          8 as _column_position
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
          9 as _column_position
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
  