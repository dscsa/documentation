
    with source_data as (
      select
        *
      from "datawarehouse".cortex."v2_accounts"
      
    ),

    column_profiles as (
      
        
        select 
          lower('id') as column_name,
          nullif(lower('bigint'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "id") as distinct_count,
          count(distinct "id") = count(*) as is_unique,
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
          lower('v2_id') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "v2_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "v2_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "v2_id") as distinct_count,
          count(distinct "v2_id") = count(*) as is_unique,
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
          lower('name') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "name" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "name") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "name") as distinct_count,
          count(distinct "name") = count(*) as is_unique,
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
          lower('license') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "license" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "license") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "license") as distinct_count,
          count(distinct "license") = count(*) as is_unique,
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
          lower('street') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "street" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "street") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "street") as distinct_count,
          count(distinct "street") = count(*) as is_unique,
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
          lower('city') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "city" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "city") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "city") as distinct_count,
          count(distinct "city") = count(*) as is_unique,
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
          lower('state') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "state" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "state") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "state") as distinct_count,
          count(distinct "state") = count(*) as is_unique,
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
          lower('zip') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "zip" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "zip") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "zip") as distinct_count,
          count(distinct "zip") = count(*) as is_unique,
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
          lower('phone') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "phone" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "phone") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "phone") as distinct_count,
          count(distinct "phone") = count(*) as is_unique,
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
          lower('default_max_inventory') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "default_max_inventory" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "default_max_inventory") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "default_max_inventory") as distinct_count,
          count(distinct "default_max_inventory") = count(*) as is_unique,
          cast(min("default_max_inventory") as varchar) as min,
          cast(max("default_max_inventory") as varchar) as max,
          avg("default_max_inventory") as avg,
          stddev_pop("default_max_inventory") as std_dev_population,
          stddev_samp("default_max_inventory") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          10 as _column_position
        from source_data

        union all
      
        
        select 
          lower('default_min_days') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "default_min_days" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "default_min_days") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "default_min_days") as distinct_count,
          count(distinct "default_min_days") = count(*) as is_unique,
          cast(min("default_min_days") as varchar) as min,
          cast(max("default_min_days") as varchar) as max,
          avg("default_min_days") as avg,
          stddev_pop("default_min_days") as std_dev_population,
          stddev_samp("default_min_days") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          11 as _column_position
        from source_data

        union all
      
        
        select 
          lower('default_min_qty') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "default_min_qty" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "default_min_qty") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "default_min_qty") as distinct_count,
          count(distinct "default_min_qty") = count(*) as is_unique,
          cast(min("default_min_qty") as varchar) as min,
          cast(max("default_min_qty") as varchar) as max,
          avg("default_min_qty") as avg,
          stddev_pop("default_min_qty") as std_dev_population,
          stddev_samp("default_min_qty") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          12 as _column_position
        from source_data

        union all
      
        
        select 
          lower('default_price_90') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "default_price_90" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "default_price_90") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "default_price_90") as distinct_count,
          count(distinct "default_price_90") = count(*) as is_unique,
          cast(min("default_price_90") as varchar) as min,
          cast(max("default_price_90") as varchar) as max,
          avg("default_price_90") as avg,
          stddev_pop("default_price_90") as std_dev_population,
          stddev_samp("default_price_90") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          13 as _column_position
        from source_data

        union all
      
        
        select 
          lower('default_price_30') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "default_price_30" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "default_price_30") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "default_price_30") as distinct_count,
          count(distinct "default_price_30") = count(*) as is_unique,
          cast(min("default_price_30") as varchar) as min,
          cast(max("default_price_30") as varchar) as max,
          avg("default_price_30") as avg,
          stddev_pop("default_price_30") as std_dev_population,
          stddev_samp("default_price_30") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          14 as _column_position
        from source_data

        union all
      
        
        select 
          lower('default_repack_quantity') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "default_repack_quantity" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "default_repack_quantity") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "default_repack_quantity") as distinct_count,
          count(distinct "default_repack_quantity") = count(*) as is_unique,
          cast(min("default_repack_quantity") as varchar) as min,
          cast(max("default_repack_quantity") as varchar) as max,
          avg("default_repack_quantity") as avg,
          stddev_pop("default_repack_quantity") as std_dev_population,
          stddev_samp("default_repack_quantity") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          15 as _column_position
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
  