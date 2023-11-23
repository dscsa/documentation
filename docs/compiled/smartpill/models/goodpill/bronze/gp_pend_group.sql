select
    cast(jsonb_extract_path_text(_airbyte_data, 'invoice_number') as int) as invoice_number,
    cast(jsonb_extract_path_text(_airbyte_data, 'pend_group_name') as varchar) as pend_group_name,
    cast(jsonb_extract_path_text(_airbyte_data, 'initial_pend_date') as timestamp) as initial_pend_date,
    cast(jsonb_extract_path_text(_airbyte_data, 'last_pend_date') as timestamp) as last_pend_date
from
    "datawarehouse"."raw"._airbyte_raw_goodpill_gp_pend_group