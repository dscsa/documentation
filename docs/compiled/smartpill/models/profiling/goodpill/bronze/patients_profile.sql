
    with source_data as (
      select
        *
      from "datawarehouse".goodpill."patients"
      
    ),

    column_profiles as (
      
        
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
          1 as _column_position
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
          2 as _column_position
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
          3 as _column_position
        from source_data

        union all
      
        
        select 
          lower('birth_date') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "birth_date" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "birth_date") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "birth_date") as distinct_count,
          count(distinct "birth_date") = count(*) as is_unique,
          cast(min("birth_date") as varchar) as min,
          cast(max("birth_date") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          4 as _column_position
        from source_data

        union all
      
        
        select 
          lower('language') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "language" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "language") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "language") as distinct_count,
          count(distinct "language") = count(*) as is_unique,
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
          lower('phone1') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "phone1" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "phone1") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "phone1") as distinct_count,
          count(distinct "phone1") = count(*) as is_unique,
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
          lower('phone2') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "phone2" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "phone2") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "phone2") as distinct_count,
          count(distinct "phone2") = count(*) as is_unique,
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
          lower('patient_address1') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_address1" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_address1") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_address1") as distinct_count,
          count(distinct "patient_address1") = count(*) as is_unique,
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
          lower('patient_address2') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_address2" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_address2") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_address2") as distinct_count,
          count(distinct "patient_address2") = count(*) as is_unique,
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
          lower('patient_city') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_city" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_city") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_city") as distinct_count,
          count(distinct "patient_city") = count(*) as is_unique,
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
          lower('patient_state') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_state" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_state") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_state") as distinct_count,
          count(distinct "patient_state") = count(*) as is_unique,
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
          lower('patient_zip') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_zip" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_zip") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_zip") as distinct_count,
          count(distinct "patient_zip") = count(*) as is_unique,
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
          lower('payment_card_type') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "payment_card_type" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "payment_card_type") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "payment_card_type") as distinct_count,
          count(distinct "payment_card_type") = count(*) as is_unique,
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
          lower('payment_card_last4') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "payment_card_last4" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "payment_card_last4") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "payment_card_last4") as distinct_count,
          count(distinct "payment_card_last4") = count(*) as is_unique,
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
          lower('payment_card_date_expired') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "payment_card_date_expired" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "payment_card_date_expired") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "payment_card_date_expired") as distinct_count,
          count(distinct "payment_card_date_expired") = count(*) as is_unique,
          cast(min("payment_card_date_expired") as varchar) as min,
          cast(max("payment_card_date_expired") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          15 as _column_position
        from source_data

        union all
      
        
        select 
          lower('payment_card_autopay') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "payment_card_autopay" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "payment_card_autopay") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "payment_card_autopay") as distinct_count,
          count(distinct "payment_card_autopay") = count(*) as is_unique,
          cast(min("payment_card_autopay") as varchar) as min,
          cast(max("payment_card_autopay") as varchar) as max,
          avg("payment_card_autopay") as avg,
          stddev_pop("payment_card_autopay") as std_dev_population,
          stddev_samp("payment_card_autopay") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          16 as _column_position
        from source_data

        union all
      
        
        select 
          lower('payment_method_default') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "payment_method_default" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "payment_method_default") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "payment_method_default") as distinct_count,
          count(distinct "payment_method_default") = count(*) as is_unique,
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
          lower('payment_coupon') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "payment_coupon" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "payment_coupon") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "payment_coupon") as distinct_count,
          count(distinct "payment_coupon") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          18 as _column_position
        from source_data

        union all
      
        
        select 
          lower('tracking_coupon') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "tracking_coupon" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "tracking_coupon") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "tracking_coupon") as distinct_count,
          count(distinct "tracking_coupon") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          19 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_date_registered') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_date_registered" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_date_registered") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_date_registered") as distinct_count,
          count(distinct "patient_date_registered") = count(*) as is_unique,
          cast(min("patient_date_registered") as varchar) as min,
          cast(max("patient_date_registered") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          20 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_date_reviewed') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_date_reviewed" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_date_reviewed") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_date_reviewed") as distinct_count,
          count(distinct "patient_date_reviewed") = count(*) as is_unique,
          cast(min("patient_date_reviewed") as varchar) as min,
          cast(max("patient_date_reviewed") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          21 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_date_added') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_date_added" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_date_added") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_date_added") as distinct_count,
          count(distinct "patient_date_added") = count(*) as is_unique,
          cast(min("patient_date_added") as varchar) as min,
          cast(max("patient_date_added") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          22 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_date_changed') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_date_changed" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_date_changed") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_date_changed") as distinct_count,
          count(distinct "patient_date_changed") = count(*) as is_unique,
          cast(min("patient_date_changed") as varchar) as min,
          cast(max("patient_date_changed") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          23 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_date_first_rx_received') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_date_first_rx_received" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_date_first_rx_received") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_date_first_rx_received") as distinct_count,
          count(distinct "patient_date_first_rx_received") = count(*) as is_unique,
          cast(min("patient_date_first_rx_received") as varchar) as min,
          cast(max("patient_date_first_rx_received") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          24 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_date_first_dispensed') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_date_first_dispensed" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_date_first_dispensed") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_date_first_dispensed") as distinct_count,
          count(distinct "patient_date_first_dispensed") = count(*) as is_unique,
          cast(min("patient_date_first_dispensed") as varchar) as min,
          cast(max("patient_date_first_dispensed") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          25 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_date_first_expected_by') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_date_first_expected_by" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_date_first_expected_by") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_date_first_expected_by") as distinct_count,
          count(distinct "patient_date_first_expected_by") = count(*) as is_unique,
          cast(min("patient_date_first_expected_by") as varchar) as min,
          cast(max("patient_date_first_expected_by") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          26 as _column_position
        from source_data

        union all
      
        
        select 
          lower('refills_used') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "refills_used" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "refills_used") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "refills_used") as distinct_count,
          count(distinct "refills_used") = count(*) as is_unique,
          cast(min("refills_used") as varchar) as min,
          cast(max("refills_used") as varchar) as max,
          avg("refills_used") as avg,
          stddev_pop("refills_used") as std_dev_population,
          stddev_samp("refills_used") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          27 as _column_position
        from source_data

        union all
      
        
        select 
          lower('pharmacy_npi') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "pharmacy_npi" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "pharmacy_npi") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "pharmacy_npi") as distinct_count,
          count(distinct "pharmacy_npi") = count(*) as is_unique,
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
          lower('pharmacy_name') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "pharmacy_name" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "pharmacy_name") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "pharmacy_name") as distinct_count,
          count(distinct "pharmacy_name") = count(*) as is_unique,
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
          lower('pharmacy_phone') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "pharmacy_phone" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "pharmacy_phone") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "pharmacy_phone") as distinct_count,
          count(distinct "pharmacy_phone") = count(*) as is_unique,
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
          lower('pharmacy_fax') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "pharmacy_fax" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "pharmacy_fax") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "pharmacy_fax") as distinct_count,
          count(distinct "pharmacy_fax") = count(*) as is_unique,
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
          lower('pharmacy_address') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "pharmacy_address" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "pharmacy_address") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "pharmacy_address") as distinct_count,
          count(distinct "pharmacy_address") = count(*) as is_unique,
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
          lower('patient_inactive') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_inactive" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_inactive") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_inactive") as distinct_count,
          count(distinct "patient_inactive") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          33 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_deleted') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_deleted" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_deleted") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_deleted") as distinct_count,
          count(distinct "patient_deleted") = count(*) as is_unique,
          cast(min("patient_deleted") as varchar) as min,
          cast(max("patient_deleted") as varchar) as max,
          avg("patient_deleted") as avg,
          stddev_pop("patient_deleted") as std_dev_population,
          stddev_samp("patient_deleted") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          34 as _column_position
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
          35 as _column_position
        from source_data

        union all
      
        
        select 
          lower('email') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "email" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "email") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "email") as distinct_count,
          count(distinct "email") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          36 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_autofill') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_autofill" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_autofill") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_autofill") as distinct_count,
          count(distinct "patient_autofill") = count(*) as is_unique,
          cast(min("patient_autofill") as varchar) as min,
          cast(max("patient_autofill") as varchar) as max,
          avg("patient_autofill") as avg,
          stddev_pop("patient_autofill") as std_dev_population,
          stddev_samp("patient_autofill") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          37 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_note') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_note" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_note") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_note") as distinct_count,
          count(distinct "patient_note") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          38 as _column_position
        from source_data

        union all
      
        
        select 
          lower('initial_invoice_number') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "initial_invoice_number" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "initial_invoice_number") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "initial_invoice_number") as distinct_count,
          count(distinct "initial_invoice_number") = count(*) as is_unique,
          cast(min("initial_invoice_number") as varchar) as min,
          cast(max("initial_invoice_number") as varchar) as max,
          avg("initial_invoice_number") as avg,
          stddev_pop("initial_invoice_number") as std_dev_population,
          stddev_samp("initial_invoice_number") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          39 as _column_position
        from source_data

        union all
      
        
        select 
          lower('allergies_none') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "allergies_none" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "allergies_none") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "allergies_none") as distinct_count,
          count(distinct "allergies_none") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          40 as _column_position
        from source_data

        union all
      
        
        select 
          lower('allergies_cephalosporins') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "allergies_cephalosporins" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "allergies_cephalosporins") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "allergies_cephalosporins") as distinct_count,
          count(distinct "allergies_cephalosporins") = count(*) as is_unique,
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
          lower('allergies_sulfa') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "allergies_sulfa" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "allergies_sulfa") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "allergies_sulfa") as distinct_count,
          count(distinct "allergies_sulfa") = count(*) as is_unique,
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
          lower('allergies_aspirin') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "allergies_aspirin" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "allergies_aspirin") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "allergies_aspirin") as distinct_count,
          count(distinct "allergies_aspirin") = count(*) as is_unique,
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
          lower('allergies_penicillin') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "allergies_penicillin" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "allergies_penicillin") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "allergies_penicillin") as distinct_count,
          count(distinct "allergies_penicillin") = count(*) as is_unique,
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
          lower('allergies_erythromycin') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "allergies_erythromycin" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "allergies_erythromycin") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "allergies_erythromycin") as distinct_count,
          count(distinct "allergies_erythromycin") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          45 as _column_position
        from source_data

        union all
      
        
        select 
          lower('allergies_codeine') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "allergies_codeine" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "allergies_codeine") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "allergies_codeine") as distinct_count,
          count(distinct "allergies_codeine") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          46 as _column_position
        from source_data

        union all
      
        
        select 
          lower('allergies_nsaids') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "allergies_nsaids" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "allergies_nsaids") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "allergies_nsaids") as distinct_count,
          count(distinct "allergies_nsaids") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          47 as _column_position
        from source_data

        union all
      
        
        select 
          lower('allergies_salicylates') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "allergies_salicylates" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "allergies_salicylates") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "allergies_salicylates") as distinct_count,
          count(distinct "allergies_salicylates") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          48 as _column_position
        from source_data

        union all
      
        
        select 
          lower('allergies_azithromycin') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "allergies_azithromycin" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "allergies_azithromycin") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "allergies_azithromycin") as distinct_count,
          count(distinct "allergies_azithromycin") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          49 as _column_position
        from source_data

        union all
      
        
        select 
          lower('allergies_amoxicillin') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "allergies_amoxicillin" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "allergies_amoxicillin") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "allergies_amoxicillin") as distinct_count,
          count(distinct "allergies_amoxicillin") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          50 as _column_position
        from source_data

        union all
      
        
        select 
          lower('allergies_tetracycline') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "allergies_tetracycline" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "allergies_tetracycline") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "allergies_tetracycline") as distinct_count,
          count(distinct "allergies_tetracycline") = count(*) as is_unique,
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
          lower('allergies_other') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "allergies_other" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "allergies_other") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "allergies_other") as distinct_count,
          count(distinct "allergies_other") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          52 as _column_position
        from source_data

        union all
      
        
        select 
          lower('medications_other') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "medications_other" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "medications_other") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "medications_other") as distinct_count,
          count(distinct "medications_other") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          53 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_date_updated') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_date_updated" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_date_updated") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_date_updated") as distinct_count,
          count(distinct "patient_date_updated") = count(*) as is_unique,
          cast(min("patient_date_updated") as varchar) as min,
          cast(max("patient_date_updated") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          54 as _column_position
        from source_data

        union all
      
        
        select 
          lower('third_party_id') as column_name,
          nullif(lower('bigint'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "third_party_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "third_party_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "third_party_id") as distinct_count,
          count(distinct "third_party_id") = count(*) as is_unique,
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
          lower('terms_viewed_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "terms_viewed_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "terms_viewed_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "terms_viewed_at") as distinct_count,
          count(distinct "terms_viewed_at") = count(*) as is_unique,
          cast(min("terms_viewed_at") as varchar) as min,
          cast(max("terms_viewed_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          56 as _column_position
        from source_data

        union all
      
        
        select 
          lower('terms_accepted') as column_name,
          nullif(lower('boolean'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "terms_accepted" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "terms_accepted") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "terms_accepted") as distinct_count,
          count(distinct "terms_accepted") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          57 as _column_position
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
  