
    with source_data as (
      select
        *
      from "datawarehouse".goodpill."order_items"
      
    ),

    column_profiles as (
      
        
        select 
          lower('invoice_number') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "invoice_number" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "invoice_number") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "invoice_number") as distinct_count,
          count(distinct "invoice_number") = count(*) as is_unique,
          cast(min("invoice_number") as varchar) as min,
          cast(max("invoice_number") as varchar) as max,
          avg("invoice_number") as avg,
          stddev_pop("invoice_number") as std_dev_population,
          stddev_samp("invoice_number") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          1 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_id_cp') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_id_cp" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_id_cp") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_id_cp") as distinct_count,
          count(distinct "patient_id_cp") = count(*) as is_unique,
          cast(min("patient_id_cp") as varchar) as min,
          cast(max("patient_id_cp") as varchar) as max,
          avg("patient_id_cp") as avg,
          stddev_pop("patient_id_cp") as std_dev_population,
          stddev_samp("patient_id_cp") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          2 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_number') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_number" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_number") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_number") as distinct_count,
          count(distinct "rx_number") = count(*) as is_unique,
          cast(min("rx_number") as varchar) as min,
          cast(max("rx_number") as varchar) as max,
          avg("rx_number") as avg,
          stddev_pop("rx_number") as std_dev_population,
          stddev_samp("rx_number") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          3 as _column_position
        from source_data

        union all
      
        
        select 
          lower('line_id') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "line_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "line_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "line_id") as distinct_count,
          count(distinct "line_id") = count(*) as is_unique,
          cast(min("line_id") as varchar) as min,
          cast(max("line_id") as varchar) as max,
          avg("line_id") as avg,
          stddev_pop("line_id") as std_dev_population,
          stddev_samp("line_id") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          4 as _column_position
        from source_data

        union all
      
        
        select 
          lower('groups') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "groups" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "groups") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "groups") as distinct_count,
          count(distinct "groups") = count(*) as is_unique,
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
          lower('rx_dispensed_id') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_dispensed_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_dispensed_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_dispensed_id") as distinct_count,
          count(distinct "rx_dispensed_id") = count(*) as is_unique,
          cast(min("rx_dispensed_id") as varchar) as min,
          cast(max("rx_dispensed_id") as varchar) as max,
          avg("rx_dispensed_id") as avg,
          stddev_pop("rx_dispensed_id") as std_dev_population,
          stddev_samp("rx_dispensed_id") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          6 as _column_position
        from source_data

        union all
      
        
        select 
          lower('stock_level_initial') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "stock_level_initial" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "stock_level_initial") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "stock_level_initial") as distinct_count,
          count(distinct "stock_level_initial") = count(*) as is_unique,
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
          lower('rx_message_keys_initial') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_message_keys_initial" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_message_keys_initial") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_message_keys_initial") as distinct_count,
          count(distinct "rx_message_keys_initial") = count(*) as is_unique,
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
          lower('patient_autofill_initial') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_autofill_initial" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_autofill_initial") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_autofill_initial") as distinct_count,
          count(distinct "patient_autofill_initial") = count(*) as is_unique,
          cast(min("patient_autofill_initial") as varchar) as min,
          cast(max("patient_autofill_initial") as varchar) as max,
          avg("patient_autofill_initial") as avg,
          stddev_pop("patient_autofill_initial") as std_dev_population,
          stddev_samp("patient_autofill_initial") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          9 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_autofill_initial') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_autofill_initial" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_autofill_initial") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_autofill_initial") as distinct_count,
          count(distinct "rx_autofill_initial") = count(*) as is_unique,
          cast(min("rx_autofill_initial") as varchar) as min,
          cast(max("rx_autofill_initial") as varchar) as max,
          avg("rx_autofill_initial") as avg,
          stddev_pop("rx_autofill_initial") as std_dev_population,
          stddev_samp("rx_autofill_initial") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          10 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_numbers_initial') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_numbers_initial" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_numbers_initial") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_numbers_initial") as distinct_count,
          count(distinct "rx_numbers_initial") = count(*) as is_unique,
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
          lower('zscore_initial') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "zscore_initial" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "zscore_initial") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "zscore_initial") as distinct_count,
          count(distinct "zscore_initial") = count(*) as is_unique,
          cast(min("zscore_initial") as varchar) as min,
          cast(max("zscore_initial") as varchar) as max,
          avg("zscore_initial") as avg,
          stddev_pop("zscore_initial") as std_dev_population,
          stddev_samp("zscore_initial") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          12 as _column_position
        from source_data

        union all
      
        
        select 
          lower('refills_dispensed_default') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "refills_dispensed_default" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "refills_dispensed_default") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "refills_dispensed_default") as distinct_count,
          count(distinct "refills_dispensed_default") = count(*) as is_unique,
          cast(min("refills_dispensed_default") as varchar) as min,
          cast(max("refills_dispensed_default") as varchar) as max,
          avg("refills_dispensed_default") as avg,
          stddev_pop("refills_dispensed_default") as std_dev_population,
          stddev_samp("refills_dispensed_default") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          13 as _column_position
        from source_data

        union all
      
        
        select 
          lower('refills_dispensed_actual') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "refills_dispensed_actual" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "refills_dispensed_actual") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "refills_dispensed_actual") as distinct_count,
          count(distinct "refills_dispensed_actual") = count(*) as is_unique,
          cast(min("refills_dispensed_actual") as varchar) as min,
          cast(max("refills_dispensed_actual") as varchar) as max,
          avg("refills_dispensed_actual") as avg,
          stddev_pop("refills_dispensed_actual") as std_dev_population,
          stddev_samp("refills_dispensed_actual") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          14 as _column_position
        from source_data

        union all
      
        
        select 
          lower('days_dispensed_default') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "days_dispensed_default" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "days_dispensed_default") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "days_dispensed_default") as distinct_count,
          count(distinct "days_dispensed_default") = count(*) as is_unique,
          cast(min("days_dispensed_default") as varchar) as min,
          cast(max("days_dispensed_default") as varchar) as max,
          avg("days_dispensed_default") as avg,
          stddev_pop("days_dispensed_default") as std_dev_population,
          stddev_samp("days_dispensed_default") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          15 as _column_position
        from source_data

        union all
      
        
        select 
          lower('days_dispensed_actual') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "days_dispensed_actual" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "days_dispensed_actual") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "days_dispensed_actual") as distinct_count,
          count(distinct "days_dispensed_actual") = count(*) as is_unique,
          cast(min("days_dispensed_actual") as varchar) as min,
          cast(max("days_dispensed_actual") as varchar) as max,
          avg("days_dispensed_actual") as avg,
          stddev_pop("days_dispensed_actual") as std_dev_population,
          stddev_samp("days_dispensed_actual") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          16 as _column_position
        from source_data

        union all
      
        
        select 
          lower('qty_dispensed_default') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "qty_dispensed_default" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "qty_dispensed_default") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "qty_dispensed_default") as distinct_count,
          count(distinct "qty_dispensed_default") = count(*) as is_unique,
          cast(min("qty_dispensed_default") as varchar) as min,
          cast(max("qty_dispensed_default") as varchar) as max,
          avg("qty_dispensed_default") as avg,
          stddev_pop("qty_dispensed_default") as std_dev_population,
          stddev_samp("qty_dispensed_default") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          17 as _column_position
        from source_data

        union all
      
        
        select 
          lower('qty_dispensed_actual') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "qty_dispensed_actual" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "qty_dispensed_actual") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "qty_dispensed_actual") as distinct_count,
          count(distinct "qty_dispensed_actual") = count(*) as is_unique,
          cast(min("qty_dispensed_actual") as varchar) as min,
          cast(max("qty_dispensed_actual") as varchar) as max,
          avg("qty_dispensed_actual") as avg,
          stddev_pop("qty_dispensed_actual") as std_dev_population,
          stddev_samp("qty_dispensed_actual") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          18 as _column_position
        from source_data

        union all
      
        
        select 
          lower('price_dispensed_default') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "price_dispensed_default" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "price_dispensed_default") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "price_dispensed_default") as distinct_count,
          count(distinct "price_dispensed_default") = count(*) as is_unique,
          cast(min("price_dispensed_default") as varchar) as min,
          cast(max("price_dispensed_default") as varchar) as max,
          avg("price_dispensed_default") as avg,
          stddev_pop("price_dispensed_default") as std_dev_population,
          stddev_samp("price_dispensed_default") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          19 as _column_position
        from source_data

        union all
      
        
        select 
          lower('price_dispensed_actual') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "price_dispensed_actual" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "price_dispensed_actual") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "price_dispensed_actual") as distinct_count,
          count(distinct "price_dispensed_actual") = count(*) as is_unique,
          cast(min("price_dispensed_actual") as varchar) as min,
          cast(max("price_dispensed_actual") as varchar) as max,
          avg("price_dispensed_actual") as avg,
          stddev_pop("price_dispensed_actual") as std_dev_population,
          stddev_samp("price_dispensed_actual") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          20 as _column_position
        from source_data

        union all
      
        
        select 
          lower('unit_price_retail_initial') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "unit_price_retail_initial" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "unit_price_retail_initial") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "unit_price_retail_initial") as distinct_count,
          count(distinct "unit_price_retail_initial") = count(*) as is_unique,
          cast(min("unit_price_retail_initial") as varchar) as min,
          cast(max("unit_price_retail_initial") as varchar) as max,
          avg("unit_price_retail_initial") as avg,
          stddev_pop("unit_price_retail_initial") as std_dev_population,
          stddev_samp("unit_price_retail_initial") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          21 as _column_position
        from source_data

        union all
      
        
        select 
          lower('unit_price_goodrx_initial') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "unit_price_goodrx_initial" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "unit_price_goodrx_initial") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "unit_price_goodrx_initial") as distinct_count,
          count(distinct "unit_price_goodrx_initial") = count(*) as is_unique,
          cast(min("unit_price_goodrx_initial") as varchar) as min,
          cast(max("unit_price_goodrx_initial") as varchar) as max,
          avg("unit_price_goodrx_initial") as avg,
          stddev_pop("unit_price_goodrx_initial") as std_dev_population,
          stddev_samp("unit_price_goodrx_initial") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          22 as _column_position
        from source_data

        union all
      
        
        select 
          lower('unit_price_nadac_initial') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "unit_price_nadac_initial" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "unit_price_nadac_initial") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "unit_price_nadac_initial") as distinct_count,
          count(distinct "unit_price_nadac_initial") = count(*) as is_unique,
          cast(min("unit_price_nadac_initial") as varchar) as min,
          cast(max("unit_price_nadac_initial") as varchar) as max,
          avg("unit_price_nadac_initial") as avg,
          stddev_pop("unit_price_nadac_initial") as std_dev_population,
          stddev_samp("unit_price_nadac_initial") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          23 as _column_position
        from source_data

        union all
      
        
        select 
          lower('unit_price_awp_initial') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "unit_price_awp_initial" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "unit_price_awp_initial") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "unit_price_awp_initial") as distinct_count,
          count(distinct "unit_price_awp_initial") = count(*) as is_unique,
          cast(min("unit_price_awp_initial") as varchar) as min,
          cast(max("unit_price_awp_initial") as varchar) as max,
          avg("unit_price_awp_initial") as avg,
          stddev_pop("unit_price_awp_initial") as std_dev_population,
          stddev_samp("unit_price_awp_initial") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          24 as _column_position
        from source_data

        union all
      
        
        select 
          lower('qty_pended_total') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "qty_pended_total" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "qty_pended_total") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "qty_pended_total") as distinct_count,
          count(distinct "qty_pended_total") = count(*) as is_unique,
          cast(min("qty_pended_total") as varchar) as min,
          cast(max("qty_pended_total") as varchar) as max,
          avg("qty_pended_total") as avg,
          stddev_pop("qty_pended_total") as std_dev_population,
          stddev_samp("qty_pended_total") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          25 as _column_position
        from source_data

        union all
      
        
        select 
          lower('qty_pended_repacks') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "qty_pended_repacks" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "qty_pended_repacks") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "qty_pended_repacks") as distinct_count,
          count(distinct "qty_pended_repacks") = count(*) as is_unique,
          cast(min("qty_pended_repacks") as varchar) as min,
          cast(max("qty_pended_repacks") as varchar) as max,
          avg("qty_pended_repacks") as avg,
          stddev_pop("qty_pended_repacks") as std_dev_population,
          stddev_samp("qty_pended_repacks") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          26 as _column_position
        from source_data

        union all
      
        
        select 
          lower('count_pended_total') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "count_pended_total" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "count_pended_total") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "count_pended_total") as distinct_count,
          count(distinct "count_pended_total") = count(*) as is_unique,
          cast(min("count_pended_total") as varchar) as min,
          cast(max("count_pended_total") as varchar) as max,
          avg("count_pended_total") as avg,
          stddev_pop("count_pended_total") as std_dev_population,
          stddev_samp("count_pended_total") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          27 as _column_position
        from source_data

        union all
      
        
        select 
          lower('count_pended_repacks') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "count_pended_repacks" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "count_pended_repacks") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "count_pended_repacks") as distinct_count,
          count(distinct "count_pended_repacks") = count(*) as is_unique,
          cast(min("count_pended_repacks") as varchar) as min,
          cast(max("count_pended_repacks") as varchar) as max,
          avg("count_pended_repacks") as avg,
          stddev_pop("count_pended_repacks") as std_dev_population,
          stddev_samp("count_pended_repacks") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          28 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_message_keys') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_message_keys" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_message_keys") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_message_keys") as distinct_count,
          count(distinct "item_message_keys") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          29 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_message_text') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_message_text" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_message_text") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_message_text") as distinct_count,
          count(distinct "item_message_text") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          30 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_type') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_type" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_type") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_type") as distinct_count,
          count(distinct "item_type") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          31 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_added_by') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_added_by" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_added_by") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_added_by") as distinct_count,
          count(distinct "item_added_by") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          32 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_date_added') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_date_added" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_date_added") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_date_added") as distinct_count,
          count(distinct "item_date_added") = count(*) as is_unique,
          cast(min("item_date_added") as varchar) as min,
          cast(max("item_date_added") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          33 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_date_changed') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_date_changed" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_date_changed") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_date_changed") as distinct_count,
          count(distinct "item_date_changed") = count(*) as is_unique,
          cast(min("item_date_changed") as varchar) as min,
          cast(max("item_date_changed") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          34 as _column_position
        from source_data

        union all
      
        
        select 
          lower('refill_date_last') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "refill_date_last" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "refill_date_last") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "refill_date_last") as distinct_count,
          count(distinct "refill_date_last") = count(*) as is_unique,
          cast(min("refill_date_last") as varchar) as min,
          cast(max("refill_date_last") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          35 as _column_position
        from source_data

        union all
      
        
        select 
          lower('refill_date_manual') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "refill_date_manual" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "refill_date_manual") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "refill_date_manual") as distinct_count,
          count(distinct "refill_date_manual") = count(*) as is_unique,
          cast(min("refill_date_manual") as varchar) as min,
          cast(max("refill_date_manual") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          36 as _column_position
        from source_data

        union all
      
        
        select 
          lower('refill_date_default') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "refill_date_default" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "refill_date_default") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "refill_date_default") as distinct_count,
          count(distinct "refill_date_default") = count(*) as is_unique,
          cast(min("refill_date_default") as varchar) as min,
          cast(max("refill_date_default") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          37 as _column_position
        from source_data

        union all
      
        
        select 
          lower('add_user_id') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "add_user_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "add_user_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "add_user_id") as distinct_count,
          count(distinct "add_user_id") = count(*) as is_unique,
          cast(min("add_user_id") as varchar) as min,
          cast(max("add_user_id") as varchar) as max,
          avg("add_user_id") as avg,
          stddev_pop("add_user_id") as std_dev_population,
          stddev_samp("add_user_id") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          38 as _column_position
        from source_data

        union all
      
        
        select 
          lower('chg_user_id') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "chg_user_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "chg_user_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "chg_user_id") as distinct_count,
          count(distinct "chg_user_id") = count(*) as is_unique,
          cast(min("chg_user_id") as varchar) as min,
          cast(max("chg_user_id") as varchar) as max,
          avg("chg_user_id") as avg,
          stddev_pop("chg_user_id") as std_dev_population,
          stddev_samp("chg_user_id") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          39 as _column_position
        from source_data

        union all
      
        
        select 
          lower('count_lines') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "count_lines" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "count_lines") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "count_lines") as distinct_count,
          count(distinct "count_lines") = count(*) as is_unique,
          cast(min("count_lines") as varchar) as min,
          cast(max("count_lines") as varchar) as max,
          avg("count_lines") as avg,
          stddev_pop("count_lines") as std_dev_population,
          stddev_samp("count_lines") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          40 as _column_position
        from source_data

        union all
      
        
        select 
          lower('drug_generic_pended') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_generic_pended" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_generic_pended") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_generic_pended") as distinct_count,
          count(distinct "drug_generic_pended") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          41 as _column_position
        from source_data

        union all
      
        
        select 
          lower('drug_name') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_name" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_name") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_name") as distinct_count,
          count(distinct "drug_name") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          42 as _column_position
        from source_data

        union all
      
        
        select 
          lower('repacked_by') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "repacked_by" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "repacked_by") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "repacked_by") as distinct_count,
          count(distinct "repacked_by") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          43 as _column_position
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
          44 as _column_position
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
          45 as _column_position
        from source_data

        union all
      
        
        select 
          lower('days_and_message_updated_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "days_and_message_updated_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "days_and_message_updated_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "days_and_message_updated_at") as distinct_count,
          count(distinct "days_and_message_updated_at") = count(*) as is_unique,
          cast(min("days_and_message_updated_at") as varchar) as min,
          cast(max("days_and_message_updated_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          46 as _column_position
        from source_data

        union all
      
        
        select 
          lower('days_and_message_initial_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "days_and_message_initial_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "days_and_message_initial_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "days_and_message_initial_at") as distinct_count,
          count(distinct "days_and_message_initial_at") = count(*) as is_unique,
          cast(min("days_and_message_initial_at") as varchar) as min,
          cast(max("days_and_message_initial_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          47 as _column_position
        from source_data

        union all
      
        
        select 
          lower('days_pended') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "days_pended" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "days_pended") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "days_pended") as distinct_count,
          count(distinct "days_pended") = count(*) as is_unique,
          cast(min("days_pended") as varchar) as min,
          cast(max("days_pended") as varchar) as max,
          avg("days_pended") as avg,
          stddev_pop("days_pended") as std_dev_population,
          stddev_samp("days_pended") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          48 as _column_position
        from source_data

        union all
      
        
        select 
          lower('qty_per_day_pended') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "qty_per_day_pended" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "qty_per_day_pended") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "qty_per_day_pended") as distinct_count,
          count(distinct "qty_per_day_pended") = count(*) as is_unique,
          cast(min("qty_per_day_pended") as varchar) as min,
          cast(max("qty_per_day_pended") as varchar) as max,
          avg("qty_per_day_pended") as avg,
          stddev_pop("qty_per_day_pended") as std_dev_population,
          stddev_samp("qty_per_day_pended") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          49 as _column_position
        from source_data

        union all
      
        
        select 
          lower('unpended_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "unpended_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "unpended_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "unpended_at") as distinct_count,
          count(distinct "unpended_at") = count(*) as is_unique,
          cast(min("unpended_at") as varchar) as min,
          cast(max("unpended_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          50 as _column_position
        from source_data

        union all
      
        
        select 
          lower('pend_initial_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "pend_initial_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "pend_initial_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "pend_initial_at") as distinct_count,
          count(distinct "pend_initial_at") = count(*) as is_unique,
          cast(min("pend_initial_at") as varchar) as min,
          cast(max("pend_initial_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          51 as _column_position
        from source_data

        union all
      
        
        select 
          lower('pend_updated_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "pend_updated_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "pend_updated_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "pend_updated_at") as distinct_count,
          count(distinct "pend_updated_at") = count(*) as is_unique,
          cast(min("pend_updated_at") as varchar) as min,
          cast(max("pend_updated_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          52 as _column_position
        from source_data

        union all
      
        
        select 
          lower('_airbyte_emitted_at') as column_name,
          nullif(lower('timestamp with time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "_airbyte_emitted_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "_airbyte_emitted_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "_airbyte_emitted_at") as distinct_count,
          count(distinct "_airbyte_emitted_at") = count(*) as is_unique,
          cast(min("_airbyte_emitted_at") as varchar) as min,
          cast(max("_airbyte_emitted_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          53 as _column_position
        from source_data

        union all
      
        
        select 
          lower('_airbyte_ab_id') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "_airbyte_ab_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "_airbyte_ab_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "_airbyte_ab_id") as distinct_count,
          count(distinct "_airbyte_ab_id") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          54 as _column_position
        from source_data

        union all
      
        
        select 
          lower('ndc_pended') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "ndc_pended" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "ndc_pended") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "ndc_pended") as distinct_count,
          count(distinct "ndc_pended") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          55 as _column_position
        from source_data

        union all
      
        
        select 
          lower('filled_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "filled_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "filled_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "filled_at") as distinct_count,
          count(distinct "filled_at") = count(*) as is_unique,
          cast(min("filled_at") as varchar) as min,
          cast(max("filled_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          56 as _column_position
        from source_data

        union all
      
        
        select 
          lower('pend_failed_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "pend_failed_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "pend_failed_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "pend_failed_at") as distinct_count,
          count(distinct "pend_failed_at") = count(*) as is_unique,
          cast(min("pend_failed_at") as varchar) as min,
          cast(max("pend_failed_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          57 as _column_position
        from source_data

        union all
      
        
        select 
          lower('filled_by') as column_name,
          nullif(lower('bigint'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "filled_by" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "filled_by") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "filled_by") as distinct_count,
          count(distinct "filled_by") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          58 as _column_position
        from source_data

        union all
      
        
        select 
          lower('pend_retried_by') as column_name,
          nullif(lower('bigint'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "pend_retried_by" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "pend_retried_by") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "pend_retried_by") as distinct_count,
          count(distinct "pend_retried_by") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          59 as _column_position
        from source_data

        union all
      
        
        select 
          lower('status') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "status" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "status") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "status") as distinct_count,
          count(distinct "status") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          60 as _column_position
        from source_data

        union all
      
        
        select 
          lower('pend_retried_days') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "pend_retried_days" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "pend_retried_days") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "pend_retried_days") as distinct_count,
          count(distinct "pend_retried_days") = count(*) as is_unique,
          cast(min("pend_retried_days") as varchar) as min,
          cast(max("pend_retried_days") as varchar) as max,
          avg("pend_retried_days") as avg,
          stddev_pop("pend_retried_days") as std_dev_population,
          stddev_samp("pend_retried_days") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          61 as _column_position
        from source_data

        union all
      
        
        select 
          lower('pend_retried_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "pend_retried_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "pend_retried_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "pend_retried_at") as distinct_count,
          count(distinct "pend_retried_at") = count(*) as is_unique,
          cast(min("pend_retried_at") as varchar) as min,
          cast(max("pend_retried_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          62 as _column_position
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
  