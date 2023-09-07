with raw_goodpill_gp_clinic_coupons as (
    select
        cast(jsonb_extract_path_text(_airbyte_data, 'coupon_code') as varchar(20)) as coupon_code,
        cast(jsonb_extract_path_text(_airbyte_data, 'coupon_type') as varchar(20)) as coupon_type,
        cast(jsonb_extract_path_text(_airbyte_data, 'clinic_id') as int) as clinic_id,
        cast(jsonb_extract_path_text(_airbyte_data, 'verified') as boolean) as verified,
        cast(jsonb_extract_path_text(_airbyte_data, 'coupon_date_used_first') as timestamp) as coupon_date_used_first,
        cast(jsonb_extract_path_text(_airbyte_data, 'coupon_date_used_last') as timestamp) as coupon_date_used_last,
        cast(jsonb_extract_path_text(_airbyte_data, 'created_at') as timestamp) as created_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'updated_at') as timestamp) as updated_at
    from
        "datawarehouse"."raw"._airbyte_raw_goodpill_gp_clinic_coupons
)

select
    nullif(coupon_code, '') as coupon_code,
    nullif(coupon_type, '') as coupon_type,
    clinic_id,
    verified,
    coupon_date_used_first,
    coupon_date_used_last,
    created_at,
    updated_at
from raw_goodpill_gp_clinic_coupons
