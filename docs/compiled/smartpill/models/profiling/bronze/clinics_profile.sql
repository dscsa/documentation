
    with source_data as (
      select
        *
      from "datawarehouse".goodpill."clinics"
      
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
          2 as _column_position
        from source_data

        union all
      
        
        select 
          lower('clinic_rx_date_added_first') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "clinic_rx_date_added_first" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "clinic_rx_date_added_first") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "clinic_rx_date_added_first") as distinct_count,
          count(distinct "clinic_rx_date_added_first") = count(*) as is_unique,
          cast(min("clinic_rx_date_added_first") as varchar) as min,
          cast(max("clinic_rx_date_added_first") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          3 as _column_position
        from source_data

        union all
      
        
        select 
          lower('clinic_rx_date_added_last') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "clinic_rx_date_added_last" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "clinic_rx_date_added_last") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "clinic_rx_date_added_last") as distinct_count,
          count(distinct "clinic_rx_date_added_last") = count(*) as is_unique,
          cast(min("clinic_rx_date_added_last") as varchar) as min,
          cast(max("clinic_rx_date_added_last") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          4 as _column_position
        from source_data

        union all
      
        
        select 
          lower('verified') as column_name,
          nullif(lower('boolean'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "verified" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "verified") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "verified") as distinct_count,
          count(distinct "verified") = count(*) as is_unique,
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
  