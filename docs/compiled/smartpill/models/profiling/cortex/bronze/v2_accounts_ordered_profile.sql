
    with source_data as (
      select
        *
      from "datawarehouse".cortex."v2_accounts_ordered"
      
    ),

    column_profiles as (
      
        
        select 
          lower('id') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "id") as distinct_count,
          count(distinct "id") = count(*) as is_unique,
          cast(min("id") as varchar) as min,
          cast(max("id") as varchar) as max,
          avg("id") as avg,
          stddev_pop("id") as std_dev_population,
          stddev_samp("id") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          1 as _column_position
        from source_data

        union all
      
        
        select 
          lower('v2_account_id') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "v2_account_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "v2_account_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "v2_account_id") as distinct_count,
          count(distinct "v2_account_id") = count(*) as is_unique,
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
          lower('drug_generic') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_generic" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_generic") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_generic") as distinct_count,
          count(distinct "drug_generic") = count(*) as is_unique,
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
          lower('default_location') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "default_location" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "default_location") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "default_location") as distinct_count,
          count(distinct "default_location") = count(*) as is_unique,
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
          lower('display_message') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "display_message" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "display_message") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "display_message") as distinct_count,
          count(distinct "display_message") = count(*) as is_unique,
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
          lower('destroyed_message') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "destroyed_message" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "destroyed_message") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "destroyed_message") as distinct_count,
          count(distinct "destroyed_message") = count(*) as is_unique,
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
          lower('max_inventory') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "max_inventory" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "max_inventory") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "max_inventory") as distinct_count,
          count(distinct "max_inventory") = count(*) as is_unique,
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
          lower('min_days') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "min_days" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "min_days") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "min_days") as distinct_count,
          count(distinct "min_days") = count(*) as is_unique,
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
          lower('min_qty') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "min_qty" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "min_qty") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "min_qty") as distinct_count,
          count(distinct "min_qty") = count(*) as is_unique,
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
          lower('price30') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "price30" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "price30") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "price30") as distinct_count,
          count(distinct "price30") = count(*) as is_unique,
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
          lower('price90') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "price90" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "price90") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "price90") as distinct_count,
          count(distinct "price90") = count(*) as is_unique,
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
          lower('repack_qty') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "repack_qty" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "repack_qty") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "repack_qty") as distinct_count,
          count(distinct "repack_qty") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          12 as _column_position
        from source_data

        union all
      
        
        select 
          lower('verified_message') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "verified_message" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "verified_message") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "verified_message") as distinct_count,
          count(distinct "verified_message") = count(*) as is_unique,
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
          lower('vial_qty') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "vial_qty" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "vial_qty") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "vial_qty") as distinct_count,
          count(distinct "vial_qty") = count(*) as is_unique,
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
          lower('vial_size') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "vial_size" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "vial_size") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "vial_size") as distinct_count,
          count(distinct "vial_size") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
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
  