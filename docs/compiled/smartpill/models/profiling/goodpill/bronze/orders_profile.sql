
    with source_data as (
      select
        *
      from "datawarehouse".goodpill."orders"
      
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
          lower('patient_id_wc') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_id_wc" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_id_wc") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_id_wc") as distinct_count,
          count(distinct "patient_id_wc") = count(*) as is_unique,
          cast(min("patient_id_wc") as varchar) as min,
          cast(max("patient_id_wc") as varchar) as max,
          avg("patient_id_wc") as avg,
          stddev_pop("patient_id_wc") as std_dev_population,
          stddev_samp("patient_id_wc") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          3 as _column_position
        from source_data

        union all
      
        
        select 
          lower('count_items') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "count_items" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "count_items") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "count_items") as distinct_count,
          count(distinct "count_items") = count(*) as is_unique,
          cast(min("count_items") as varchar) as min,
          cast(max("count_items") as varchar) as max,
          avg("count_items") as avg,
          stddev_pop("count_items") as std_dev_population,
          stddev_samp("count_items") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          4 as _column_position
        from source_data

        union all
      
        
        select 
          lower('count_filled') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "count_filled" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "count_filled") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "count_filled") as distinct_count,
          count(distinct "count_filled") = count(*) as is_unique,
          cast(min("count_filled") as varchar) as min,
          cast(max("count_filled") as varchar) as max,
          avg("count_filled") as avg,
          stddev_pop("count_filled") as std_dev_population,
          stddev_samp("count_filled") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          5 as _column_position
        from source_data

        union all
      
        
        select 
          lower('count_nofill') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "count_nofill" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "count_nofill") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "count_nofill") as distinct_count,
          count(distinct "count_nofill") = count(*) as is_unique,
          cast(min("count_nofill") as varchar) as min,
          cast(max("count_nofill") as varchar) as max,
          avg("count_nofill") as avg,
          stddev_pop("count_nofill") as std_dev_population,
          stddev_samp("count_nofill") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          6 as _column_position
        from source_data

        union all
      
        
        select 
          lower('priority') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "priority" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "priority") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "priority") as distinct_count,
          count(distinct "priority") = count(*) as is_unique,
          cast(min("priority") as varchar) as min,
          cast(max("priority") as varchar) as max,
          avg("priority") as avg,
          stddev_pop("priority") as std_dev_population,
          stddev_samp("priority") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          7 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_source') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_source" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_source") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_source") as distinct_count,
          count(distinct "order_source") = count(*) as is_unique,
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
          lower('order_stage_cp') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_stage_cp" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_stage_cp") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_stage_cp") as distinct_count,
          count(distinct "order_stage_cp") = count(*) as is_unique,
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
          lower('order_stage_wc') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_stage_wc" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_stage_wc") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_stage_wc") as distinct_count,
          count(distinct "order_stage_wc") = count(*) as is_unique,
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
          lower('order_status') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_status" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_status") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_status") as distinct_count,
          count(distinct "order_status") = count(*) as is_unique,
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
          lower('order_address1') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_address1" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_address1") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_address1") as distinct_count,
          count(distinct "order_address1") = count(*) as is_unique,
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
          lower('order_address2') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_address2" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_address2") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_address2") as distinct_count,
          count(distinct "order_address2") = count(*) as is_unique,
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
          lower('invoice_doc_id') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "invoice_doc_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "invoice_doc_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "invoice_doc_id") as distinct_count,
          count(distinct "invoice_doc_id") = count(*) as is_unique,
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
          lower('tracking_number') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "tracking_number" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "tracking_number") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "tracking_number") as distinct_count,
          count(distinct "tracking_number") = count(*) as is_unique,
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
          lower('payment_total_default') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "payment_total_default" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "payment_total_default") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "payment_total_default") as distinct_count,
          count(distinct "payment_total_default") = count(*) as is_unique,
          cast(min("payment_total_default") as varchar) as min,
          cast(max("payment_total_default") as varchar) as max,
          avg("payment_total_default") as avg,
          stddev_pop("payment_total_default") as std_dev_population,
          stddev_samp("payment_total_default") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          16 as _column_position
        from source_data

        union all
      
        
        select 
          lower('payment_total_actual') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "payment_total_actual" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "payment_total_actual") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "payment_total_actual") as distinct_count,
          count(distinct "payment_total_actual") = count(*) as is_unique,
          cast(min("payment_total_actual") as varchar) as min,
          cast(max("payment_total_actual") as varchar) as max,
          avg("payment_total_actual") as avg,
          stddev_pop("payment_total_actual") as std_dev_population,
          stddev_samp("payment_total_actual") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          17 as _column_position
        from source_data

        union all
      
        
        select 
          lower('payment_fee_default') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "payment_fee_default" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "payment_fee_default") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "payment_fee_default") as distinct_count,
          count(distinct "payment_fee_default") = count(*) as is_unique,
          cast(min("payment_fee_default") as varchar) as min,
          cast(max("payment_fee_default") as varchar) as max,
          avg("payment_fee_default") as avg,
          stddev_pop("payment_fee_default") as std_dev_population,
          stddev_samp("payment_fee_default") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          18 as _column_position
        from source_data

        union all
      
        
        select 
          lower('payment_fee_actual') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "payment_fee_actual" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "payment_fee_actual") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "payment_fee_actual") as distinct_count,
          count(distinct "payment_fee_actual") = count(*) as is_unique,
          cast(min("payment_fee_actual") as varchar) as min,
          cast(max("payment_fee_actual") as varchar) as max,
          avg("payment_fee_actual") as avg,
          stddev_pop("payment_fee_actual") as std_dev_population,
          stddev_samp("payment_fee_actual") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          19 as _column_position
        from source_data

        union all
      
        
        select 
          lower('payment_due_default') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "payment_due_default" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "payment_due_default") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "payment_due_default") as distinct_count,
          count(distinct "payment_due_default") = count(*) as is_unique,
          cast(min("payment_due_default") as varchar) as min,
          cast(max("payment_due_default") as varchar) as max,
          avg("payment_due_default") as avg,
          stddev_pop("payment_due_default") as std_dev_population,
          stddev_samp("payment_due_default") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          20 as _column_position
        from source_data

        union all
      
        
        select 
          lower('payment_due_actual') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "payment_due_actual" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "payment_due_actual") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "payment_due_actual") as distinct_count,
          count(distinct "payment_due_actual") = count(*) as is_unique,
          cast(min("payment_due_actual") as varchar) as min,
          cast(max("payment_due_actual") as varchar) as max,
          avg("payment_due_actual") as avg,
          stddev_pop("payment_due_actual") as std_dev_population,
          stddev_samp("payment_due_actual") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          21 as _column_position
        from source_data

        union all
      
        
        select 
          lower('payment_default_updated_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "payment_default_updated_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "payment_default_updated_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "payment_default_updated_at") as distinct_count,
          count(distinct "payment_default_updated_at") = count(*) as is_unique,
          cast(min("payment_default_updated_at") as varchar) as min,
          cast(max("payment_default_updated_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          22 as _column_position
        from source_data

        union all
      
        
        select 
          lower('payment_actual_updated_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "payment_actual_updated_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "payment_actual_updated_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "payment_actual_updated_at") as distinct_count,
          count(distinct "payment_actual_updated_at") = count(*) as is_unique,
          cast(min("payment_actual_updated_at") as varchar) as min,
          cast(max("payment_actual_updated_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          23 as _column_position
        from source_data

        union all
      
        
        select 
          lower('payment_date_autopay') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "payment_date_autopay" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "payment_date_autopay") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "payment_date_autopay") as distinct_count,
          count(distinct "payment_date_autopay") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          24 as _column_position
        from source_data

        union all
      
        
        select 
          lower('payment_method_actual') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "payment_method_actual" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "payment_method_actual") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "payment_method_actual") as distinct_count,
          count(distinct "payment_method_actual") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          25 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_payment_coupon') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_payment_coupon" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_payment_coupon") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_payment_coupon") as distinct_count,
          count(distinct "order_payment_coupon") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          26 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_note') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_note" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_note") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_note") as distinct_count,
          count(distinct "order_note") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          27 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rph_check') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rph_check" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rph_check") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rph_check") as distinct_count,
          count(distinct "rph_check") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          28 as _column_position
        from source_data

        union all
      
        
        select 
          lower('tech_fill') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "tech_fill" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "tech_fill") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "tech_fill") as distinct_count,
          count(distinct "tech_fill") = count(*) as is_unique,
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
          lower('order_date_returned') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_date_returned" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_date_returned") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_date_returned") as distinct_count,
          count(distinct "order_date_returned") = count(*) as is_unique,
          cast(min("order_date_returned") as varchar) as min,
          cast(max("order_date_returned") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          30 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_date_shipped') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_date_shipped" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_date_shipped") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_date_shipped") as distinct_count,
          count(distinct "order_date_shipped") = count(*) as is_unique,
          cast(min("order_date_shipped") as varchar) as min,
          cast(max("order_date_shipped") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          31 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_date_dispensed') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_date_dispensed" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_date_dispensed") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_date_dispensed") as distinct_count,
          count(distinct "order_date_dispensed") = count(*) as is_unique,
          cast(min("order_date_dispensed") as varchar) as min,
          cast(max("order_date_dispensed") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          32 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_date_added') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_date_added" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_date_added") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_date_added") as distinct_count,
          count(distinct "order_date_added") = count(*) as is_unique,
          cast(min("order_date_added") as varchar) as min,
          cast(max("order_date_added") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          33 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_date_changed') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_date_changed" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_date_changed") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_date_changed") as distinct_count,
          count(distinct "order_date_changed") = count(*) as is_unique,
          cast(min("order_date_changed") as varchar) as min,
          cast(max("order_date_changed") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          34 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_date_delivered') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_date_delivered" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_date_delivered") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_date_delivered") as distinct_count,
          count(distinct "order_date_delivered") = count(*) as is_unique,
          cast(min("order_date_delivered") as varchar) as min,
          cast(max("order_date_delivered") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          35 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_date_expedited') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_date_expedited" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_date_expedited") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_date_expedited") as distinct_count,
          count(distinct "order_date_expedited") = count(*) as is_unique,
          cast(min("order_date_expedited") as varchar) as min,
          cast(max("order_date_expedited") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          36 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_date_expected') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_date_expected" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_date_expected") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_date_expected") as distinct_count,
          count(distinct "order_date_expected") = count(*) as is_unique,
          cast(min("order_date_expected") as varchar) as min,
          cast(max("order_date_expected") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          37 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_date_expected_initial') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_date_expected_initial" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_date_expected_initial") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_date_expected_initial") as distinct_count,
          count(distinct "order_date_expected_initial") = count(*) as is_unique,
          cast(min("order_date_expected_initial") as varchar) as min,
          cast(max("order_date_expected_initial") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          38 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_date_failed') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_date_failed" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_date_failed") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_date_failed") as distinct_count,
          count(distinct "order_date_failed") = count(*) as is_unique,
          cast(min("order_date_failed") as varchar) as min,
          cast(max("order_date_failed") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          39 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_date_updated') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_date_updated" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_date_updated") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_date_updated") as distinct_count,
          count(distinct "order_date_updated") = count(*) as is_unique,
          cast(min("order_date_updated") as varchar) as min,
          cast(max("order_date_updated") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          40 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_stage_wc_updated_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_stage_wc_updated_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_stage_wc_updated_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_stage_wc_updated_at") as distinct_count,
          count(distinct "order_stage_wc_updated_at") = count(*) as is_unique,
          cast(min("order_stage_wc_updated_at") as varchar) as min,
          cast(max("order_stage_wc_updated_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          41 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_city') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_city" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_city") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_city") as distinct_count,
          count(distinct "order_city") = count(*) as is_unique,
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
          lower('order_state') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_state" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_state") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_state") as distinct_count,
          count(distinct "order_state") = count(*) as is_unique,
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
          lower('order_zip') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_zip" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_zip") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_zip") as distinct_count,
          count(distinct "order_zip") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          44 as _column_position
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
          45 as _column_position
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
          46 as _column_position
        from source_data

        union all
      
        
        select 
          lower('shipping_speed') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "shipping_speed" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "shipping_speed") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "shipping_speed") as distinct_count,
          count(distinct "shipping_speed") = count(*) as is_unique,
          cast(min("shipping_speed") as varchar) as min,
          cast(max("shipping_speed") as varchar) as max,
          avg("shipping_speed") as avg,
          stddev_pop("shipping_speed") as std_dev_population,
          stddev_samp("shipping_speed") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          47 as _column_position
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
          48 as _column_position
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
          49 as _column_position
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
          50 as _column_position
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
          51 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_group_additions_checked_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_group_additions_checked_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_group_additions_checked_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_group_additions_checked_at") as distinct_count,
          count(distinct "rx_group_additions_checked_at") = count(*) as is_unique,
          cast(min("rx_group_additions_checked_at") as varchar) as min,
          cast(max("rx_group_additions_checked_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          52 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_group_removals_checked_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_group_removals_checked_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_group_removals_checked_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_group_removals_checked_at") as distinct_count,
          count(distinct "rx_group_removals_checked_at") = count(*) as is_unique,
          cast(min("rx_group_removals_checked_at") as varchar) as min,
          cast(max("rx_group_removals_checked_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          53 as _column_position
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
          54 as _column_position
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
  