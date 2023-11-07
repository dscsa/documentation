
    with source_data as (
      select
        *
      from "datawarehouse".cortex."v2_drugs"
      
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
          lower('form') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "form" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "form") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "form") as distinct_count,
          count(distinct "form") = count(*) as is_unique,
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
          lower('brand') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "brand" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "brand") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "brand") as distinct_count,
          count(distinct "brand") = count(*) as is_unique,
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
          lower('labeler') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "labeler" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "labeler") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "labeler") as distinct_count,
          count(distinct "labeler") = count(*) as is_unique,
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
          lower('price_goodrx') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "price_goodrx" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "price_goodrx") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "price_goodrx") as distinct_count,
          count(distinct "price_goodrx") = count(*) as is_unique,
          cast(min("price_goodrx") as varchar) as min,
          cast(max("price_goodrx") as varchar) as max,
          avg("price_goodrx") as avg,
          stddev_pop("price_goodrx") as std_dev_population,
          stddev_samp("price_goodrx") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          6 as _column_position
        from source_data

        union all
      
        
        select 
          lower('price_retail') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "price_retail" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "price_retail") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "price_retail") as distinct_count,
          count(distinct "price_retail") = count(*) as is_unique,
          cast(min("price_retail") as varchar) as min,
          cast(max("price_retail") as varchar) as max,
          avg("price_retail") as avg,
          stddev_pop("price_retail") as std_dev_population,
          stddev_samp("price_retail") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          7 as _column_position
        from source_data

        union all
      
        
        select 
          lower('price_invalid_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "price_invalid_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "price_invalid_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "price_invalid_at") as distinct_count,
          count(distinct "price_invalid_at") = count(*) as is_unique,
          cast(min("price_invalid_at") as varchar) as min,
          cast(max("price_invalid_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          8 as _column_position
        from source_data

        union all
      
        
        select 
          lower('price_updated_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "price_updated_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "price_updated_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "price_updated_at") as distinct_count,
          count(distinct "price_updated_at") = count(*) as is_unique,
          cast(min("price_updated_at") as varchar) as min,
          cast(max("price_updated_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          9 as _column_position
        from source_data

        union all
      
        
        select 
          lower('image') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "image" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "image") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "image") as distinct_count,
          count(distinct "image") = count(*) as is_unique,
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
          lower('ndc9') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "ndc9" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "ndc9") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "ndc9") as distinct_count,
          count(distinct "ndc9") = count(*) as is_unique,
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
          lower('generic') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "generic" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "generic") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "generic") as distinct_count,
          count(distinct "generic") = count(*) as is_unique,
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
          lower('upc') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "upc" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "upc") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "upc") as distinct_count,
          count(distinct "upc") = count(*) as is_unique,
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
          14 as _column_position
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
  