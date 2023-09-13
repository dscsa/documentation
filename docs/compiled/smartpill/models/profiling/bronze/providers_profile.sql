
    with source_data as (
      select
        *
      from "datawarehouse".goodpill."providers"
      
    ),

    column_profiles as (
      
        
        select 
          lower('npi') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "npi" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "npi") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "npi") as distinct_count,
          count(distinct "npi") = count(*) as is_unique,
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
          2 as _column_position
        from source_data

        union all
      
        
        select 
          lower('first_name') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "first_name" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "first_name") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "first_name") as distinct_count,
          count(distinct "first_name") = count(*) as is_unique,
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
          lower('last_name') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "last_name" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "last_name") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "last_name") as distinct_count,
          count(distinct "last_name") = count(*) as is_unique,
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
          lower('first_rx_sent_date') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "first_rx_sent_date" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "first_rx_sent_date") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "first_rx_sent_date") as distinct_count,
          count(distinct "first_rx_sent_date") = count(*) as is_unique,
          cast(min("first_rx_sent_date") as varchar) as min,
          cast(max("first_rx_sent_date") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          5 as _column_position
        from source_data

        union all
      
        
        select 
          lower('last_rx_sent_date') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "last_rx_sent_date" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "last_rx_sent_date") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "last_rx_sent_date") as distinct_count,
          count(distinct "last_rx_sent_date") = count(*) as is_unique,
          cast(min("last_rx_sent_date") as varchar) as min,
          cast(max("last_rx_sent_date") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          6 as _column_position
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
  