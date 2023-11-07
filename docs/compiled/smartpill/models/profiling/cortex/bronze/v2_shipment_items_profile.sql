
    with source_data as (
      select
        *
      from "datawarehouse".cortex."v2_shipment_items"
      
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
          lower('couch_id') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "couch_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "couch_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "couch_id") as distinct_count,
          count(distinct "couch_id") = count(*) as is_unique,
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
          lower('ndc') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "ndc" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "ndc") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "ndc") as distinct_count,
          count(distinct "ndc") = count(*) as is_unique,
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
          lower('drug_brand') as column_name,
          nullif(lower('character varying'), '') as data_type,
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
          4 as _column_position
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
          5 as _column_position
        from source_data

        union all
      
        
        select 
          lower('drug_generic_name') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_generic_name" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_generic_name") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_generic_name") as distinct_count,
          count(distinct "drug_generic_name") = count(*) as is_unique,
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
          lower('drug_generic_strength') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_generic_strength" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_generic_strength") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_generic_strength") as distinct_count,
          count(distinct "drug_generic_strength") = count(*) as is_unique,
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
          lower('drug_form') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_form" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_form") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_form") as distinct_count,
          count(distinct "drug_form") = count(*) as is_unique,
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
          lower('drug_price_goodrx') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_price_goodrx" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_price_goodrx") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_price_goodrx") as distinct_count,
          count(distinct "drug_price_goodrx") = count(*) as is_unique,
          cast(min("drug_price_goodrx") as varchar) as min,
          cast(max("drug_price_goodrx") as varchar) as max,
          avg("drug_price_goodrx") as avg,
          stddev_pop("drug_price_goodrx") as std_dev_population,
          stddev_samp("drug_price_goodrx") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          9 as _column_position
        from source_data

        union all
      
        
        select 
          lower('drug_price_nadac') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_price_nadac" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_price_nadac") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_price_nadac") as distinct_count,
          count(distinct "drug_price_nadac") = count(*) as is_unique,
          cast(min("drug_price_nadac") as varchar) as min,
          cast(max("drug_price_nadac") as varchar) as max,
          avg("drug_price_nadac") as avg,
          stddev_pop("drug_price_nadac") as std_dev_population,
          stddev_samp("drug_price_nadac") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          10 as _column_position
        from source_data

        union all
      
        
        select 
          lower('drug_price_retail') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_price_retail" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_price_retail") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_price_retail") as distinct_count,
          count(distinct "drug_price_retail") = count(*) as is_unique,
          cast(min("drug_price_retail") as varchar) as min,
          cast(max("drug_price_retail") as varchar) as max,
          avg("drug_price_retail") as avg,
          stddev_pop("drug_price_retail") as std_dev_population,
          stddev_samp("drug_price_retail") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          11 as _column_position
        from source_data

        union all
      
        
        select 
          lower('drug_price') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_price" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_price") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_price") as distinct_count,
          count(distinct "drug_price") = count(*) as is_unique,
          cast(min("drug_price") as varchar) as min,
          cast(max("drug_price") as varchar) as max,
          avg("drug_price") as avg,
          stddev_pop("drug_price") as std_dev_population,
          stddev_samp("drug_price") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          12 as _column_position
        from source_data

        union all
      
        
        select 
          lower('drug_price_updated_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_price_updated_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_price_updated_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_price_updated_at") as distinct_count,
          count(distinct "drug_price_updated_at") = count(*) as is_unique,
          cast(min("drug_price_updated_at") as varchar) as min,
          cast(max("drug_price_updated_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          13 as _column_position
        from source_data

        union all
      
        
        select 
          lower('drug_pkg') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_pkg" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_pkg") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_pkg") as distinct_count,
          count(distinct "drug_pkg") = count(*) as is_unique,
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
          lower('bin') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "bin" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "bin") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "bin") as distinct_count,
          count(distinct "bin") = count(*) as is_unique,
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
          lower('v2_user_id') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "v2_user_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "v2_user_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "v2_user_id") as distinct_count,
          count(distinct "v2_user_id") = count(*) as is_unique,
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
          lower('v2_shipment_id') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "v2_shipment_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "v2_shipment_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "v2_shipment_id") as distinct_count,
          count(distinct "v2_shipment_id") = count(*) as is_unique,
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
          lower('v2_donor_id') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "v2_donor_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "v2_donor_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "v2_donor_id") as distinct_count,
          count(distinct "v2_donor_id") = count(*) as is_unique,
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
          lower('v2_recipient_id') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "v2_recipient_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "v2_recipient_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "v2_recipient_id") as distinct_count,
          count(distinct "v2_recipient_id") = count(*) as is_unique,
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
          lower('quantity') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "quantity" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "quantity") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "quantity") as distinct_count,
          count(distinct "quantity") = count(*) as is_unique,
          cast(min("quantity") as varchar) as min,
          cast(max("quantity") as varchar) as max,
          avg("quantity") as avg,
          stddev_pop("quantity") as std_dev_population,
          stddev_samp("quantity") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          20 as _column_position
        from source_data

        union all
      
        
        select 
          lower('value') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "value" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "value") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "value") as distinct_count,
          count(distinct "value") = count(*) as is_unique,
          cast(min("value") as varchar) as min,
          cast(max("value") as varchar) as max,
          avg("value") as avg,
          stddev_pop("value") as std_dev_population,
          stddev_samp("value") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          21 as _column_position
        from source_data

        union all
      
        
        select 
          lower('expires_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "expires_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "expires_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "expires_at") as distinct_count,
          count(distinct "expires_at") = count(*) as is_unique,
          cast(min("expires_at") as varchar) as min,
          cast(max("expires_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          22 as _column_position
        from source_data

        union all
      
        
        select 
          lower('verified_on') as column_name,
          nullif(lower('date'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "verified_on" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "verified_on") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "verified_on") as distinct_count,
          count(distinct "verified_on") = count(*) as is_unique,
          cast(min("verified_on") as varchar) as min,
          cast(max("verified_on") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          23 as _column_position
        from source_data

        union all
      
        
        select 
          lower('refused_on') as column_name,
          nullif(lower('date'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "refused_on" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "refused_on") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "refused_on") as distinct_count,
          count(distinct "refused_on") = count(*) as is_unique,
          cast(min("refused_on") as varchar) as min,
          cast(max("refused_on") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          24 as _column_position
        from source_data

        union all
      
        
        select 
          lower('entered_on') as column_name,
          nullif(lower('date'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "entered_on" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "entered_on") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "entered_on") as distinct_count,
          count(distinct "entered_on") = count(*) as is_unique,
          cast(min("entered_on") as varchar) as min,
          cast(max("entered_on") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          25 as _column_position
        from source_data

        union all
      
        
        select 
          lower('expired_on') as column_name,
          nullif(lower('date'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "expired_on" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "expired_on") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "expired_on") as distinct_count,
          count(distinct "expired_on") = count(*) as is_unique,
          cast(min("expired_on") as varchar) as min,
          cast(max("expired_on") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          26 as _column_position
        from source_data

        union all
      
        
        select 
          lower('disposed_on') as column_name,
          nullif(lower('date'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "disposed_on" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "disposed_on") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "disposed_on") as distinct_count,
          count(distinct "disposed_on") = count(*) as is_unique,
          cast(min("disposed_on") as varchar) as min,
          cast(max("disposed_on") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          27 as _column_position
        from source_data

        union all
      
        
        select 
          lower('created_on') as column_name,
          nullif(lower('date'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "created_on" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "created_on") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "created_on") as distinct_count,
          count(distinct "created_on") = count(*) as is_unique,
          cast(min("created_on") as varchar) as min,
          cast(max("created_on") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          28 as _column_position
        from source_data

        union all
      
        
        select 
          lower('received_on') as column_name,
          nullif(lower('date'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "received_on" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "received_on") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "received_on") as distinct_count,
          count(distinct "received_on") = count(*) as is_unique,
          cast(min("received_on") as varchar) as min,
          cast(max("received_on") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          29 as _column_position
        from source_data

        union all
      
        
        select 
          lower('dispensed_on') as column_name,
          nullif(lower('date'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "dispensed_on" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "dispensed_on") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "dispensed_on") as distinct_count,
          count(distinct "dispensed_on") = count(*) as is_unique,
          cast(min("dispensed_on") as varchar) as min,
          cast(max("dispensed_on") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          30 as _column_position
        from source_data

        union all
      
        
        select 
          lower('picked_on') as column_name,
          nullif(lower('date'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "picked_on" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "picked_on") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "picked_on") as distinct_count,
          count(distinct "picked_on") = count(*) as is_unique,
          cast(min("picked_on") as varchar) as min,
          cast(max("picked_on") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          31 as _column_position
        from source_data

        union all
      
        
        select 
          lower('repacked_on') as column_name,
          nullif(lower('date'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "repacked_on" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "repacked_on") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "repacked_on") as distinct_count,
          count(distinct "repacked_on") = count(*) as is_unique,
          cast(min("repacked_on") as varchar) as min,
          cast(max("repacked_on") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          32 as _column_position
        from source_data

        union all
      
        
        select 
          lower('pended_on') as column_name,
          nullif(lower('date'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "pended_on" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "pended_on") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "pended_on") as distinct_count,
          count(distinct "pended_on") = count(*) as is_unique,
          cast(min("pended_on") as varchar) as min,
          cast(max("pended_on") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          33 as _column_position
        from source_data

        union all
      
        
        select 
          lower('next_on') as column_name,
          nullif(lower('date'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "next_on" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "next_on") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "next_on") as distinct_count,
          count(distinct "next_on") = count(*) as is_unique,
          cast(min("next_on") as varchar) as min,
          cast(max("next_on") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          34 as _column_position
        from source_data

        union all
      
        
        select 
          lower('removed_on') as column_name,
          nullif(lower('date'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "removed_on" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "removed_on") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "removed_on") as distinct_count,
          count(distinct "removed_on") = count(*) as is_unique,
          cast(min("removed_on") as varchar) as min,
          cast(max("removed_on") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          35 as _column_position
        from source_data

        union all
      
        
        select 
          lower('sorted_bin_1') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "sorted_bin_1" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "sorted_bin_1") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "sorted_bin_1") as distinct_count,
          count(distinct "sorted_bin_1") = count(*) as is_unique,
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
          lower('sorted_bin_2') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "sorted_bin_2" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "sorted_bin_2") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "sorted_bin_2") as distinct_count,
          count(distinct "sorted_bin_2") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          37 as _column_position
        from source_data

        union all
      
        
        select 
          lower('sorted_bin_3') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "sorted_bin_3" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "sorted_bin_3") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "sorted_bin_3") as distinct_count,
          count(distinct "sorted_bin_3") = count(*) as is_unique,
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
          lower('sorted_bin_4') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "sorted_bin_4" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "sorted_bin_4") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "sorted_bin_4") as distinct_count,
          count(distinct "sorted_bin_4") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          39 as _column_position
        from source_data

        union all
      
        
        select 
          lower('is_refused') as column_name,
          nullif(lower('boolean'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "is_refused" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "is_refused") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "is_refused") as distinct_count,
          count(distinct "is_refused") = count(*) as is_unique,
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
          lower('is_inventory') as column_name,
          nullif(lower('boolean'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "is_inventory" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "is_inventory") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "is_inventory") as distinct_count,
          count(distinct "is_inventory") = count(*) as is_unique,
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
          lower('is_pended') as column_name,
          nullif(lower('boolean'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "is_pended" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "is_pended") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "is_pended") as distinct_count,
          count(distinct "is_pended") = count(*) as is_unique,
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
          lower('is_dispensed') as column_name,
          nullif(lower('boolean'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "is_dispensed" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "is_dispensed") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "is_dispensed") as distinct_count,
          count(distinct "is_dispensed") = count(*) as is_unique,
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
          lower('is_disposed') as column_name,
          nullif(lower('boolean'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "is_disposed" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "is_disposed") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "is_disposed") as distinct_count,
          count(distinct "is_disposed") = count(*) as is_unique,
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
          lower('is_expired') as column_name,
          nullif(lower('boolean'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "is_expired" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "is_expired") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "is_expired") as distinct_count,
          count(distinct "is_expired") = count(*) as is_unique,
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
          lower('is_magic_bin') as column_name,
          nullif(lower('boolean'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "is_magic_bin" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "is_magic_bin") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "is_magic_bin") as distinct_count,
          count(distinct "is_magic_bin") = count(*) as is_unique,
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
          47 as _column_position
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
  