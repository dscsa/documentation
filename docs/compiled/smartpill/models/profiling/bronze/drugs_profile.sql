
    with source_data as (
      select
        *
      from "datawarehouse".goodpill."drugs"
      
    ),

    column_profiles as (
      
        
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
          1 as _column_position
        from source_data

        union all
      
        
        select 
          lower('drug_brand') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_brand" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_brand") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_brand") as distinct_count,
          count(distinct "drug_brand") = count(*) as is_unique,
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
          lower('drug_gsns') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_gsns" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_gsns") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_gsns") as distinct_count,
          count(distinct "drug_gsns") = count(*) as is_unique,
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
          lower('drug_ordered') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_ordered" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_ordered") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_ordered") as distinct_count,
          count(distinct "drug_ordered") = count(*) as is_unique,
          cast(min("drug_ordered") as varchar) as min,
          cast(max("drug_ordered") as varchar) as max,
          avg("drug_ordered") as avg,
          stddev_pop("drug_ordered") as std_dev_population,
          stddev_samp("drug_ordered") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          4 as _column_position
        from source_data

        union all
      
        
        select 
          lower('price30') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "price30" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "price30") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "price30") as distinct_count,
          count(distinct "price30") = count(*) as is_unique,
          cast(min("price30") as varchar) as min,
          cast(max("price30") as varchar) as max,
          avg("price30") as avg,
          stddev_pop("price30") as std_dev_population,
          stddev_samp("price30") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          5 as _column_position
        from source_data

        union all
      
        
        select 
          lower('price90') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "price90" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "price90") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "price90") as distinct_count,
          count(distinct "price90") = count(*) as is_unique,
          cast(min("price90") as varchar) as min,
          cast(max("price90") as varchar) as max,
          avg("price90") as avg,
          stddev_pop("price90") as std_dev_population,
          stddev_samp("price90") as std_dev_sample,
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
          8 as _column_position
        from source_data

        union all
      
        
        select 
          lower('price_nadac') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "price_nadac" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "price_nadac") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "price_nadac") as distinct_count,
          count(distinct "price_nadac") = count(*) as is_unique,
          cast(min("price_nadac") as varchar) as min,
          cast(max("price_nadac") as varchar) as max,
          avg("price_nadac") as avg,
          stddev_pop("price_nadac") as std_dev_population,
          stddev_samp("price_nadac") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          9 as _column_position
        from source_data

        union all
      
        
        select 
          lower('qty_min') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "qty_min" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "qty_min") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "qty_min") as distinct_count,
          count(distinct "qty_min") = count(*) as is_unique,
          cast(min("qty_min") as varchar) as min,
          cast(max("qty_min") as varchar) as max,
          avg("qty_min") as avg,
          stddev_pop("qty_min") as std_dev_population,
          stddev_samp("qty_min") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          10 as _column_position
        from source_data

        union all
      
        
        select 
          lower('qty_repack') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "qty_repack" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "qty_repack") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "qty_repack") as distinct_count,
          count(distinct "qty_repack") = count(*) as is_unique,
          cast(min("qty_repack") as varchar) as min,
          cast(max("qty_repack") as varchar) as max,
          avg("qty_repack") as avg,
          stddev_pop("qty_repack") as std_dev_population,
          stddev_samp("qty_repack") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          11 as _column_position
        from source_data

        union all
      
        
        select 
          lower('days_min') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "days_min" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "days_min") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "days_min") as distinct_count,
          count(distinct "days_min") = count(*) as is_unique,
          cast(min("days_min") as varchar) as min,
          cast(max("days_min") as varchar) as max,
          avg("days_min") as avg,
          stddev_pop("days_min") as std_dev_population,
          stddev_samp("days_min") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          12 as _column_position
        from source_data

        union all
      
        
        select 
          lower('count_ndcs') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "count_ndcs" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "count_ndcs") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "count_ndcs") as distinct_count,
          count(distinct "count_ndcs") = count(*) as is_unique,
          cast(min("count_ndcs") as varchar) as min,
          cast(max("count_ndcs") as varchar) as max,
          avg("count_ndcs") as avg,
          stddev_pop("count_ndcs") as std_dev_population,
          stddev_samp("count_ndcs") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          13 as _column_position
        from source_data

        union all
      
        
        select 
          lower('max_inventory') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "max_inventory" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "max_inventory") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "max_inventory") as distinct_count,
          count(distinct "max_inventory") = count(*) as is_unique,
          cast(min("max_inventory") as varchar) as min,
          cast(max("max_inventory") as varchar) as max,
          avg("max_inventory") as avg,
          stddev_pop("max_inventory") as std_dev_population,
          stddev_samp("max_inventory") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          14 as _column_position
        from source_data

        union all
      
        
        select 
          lower('message_display') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "message_display" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "message_display") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "message_display") as distinct_count,
          count(distinct "message_display") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          15 as _column_position
        from source_data

        union all
      
        
        select 
          lower('message_verified') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "message_verified" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "message_verified") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "message_verified") as distinct_count,
          count(distinct "message_verified") = count(*) as is_unique,
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
          lower('message_destroyed') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "message_destroyed" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "message_destroyed") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "message_destroyed") as distinct_count,
          count(distinct "message_destroyed") = count(*) as is_unique,
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
          18 as _column_position
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
          19 as _column_position
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
  