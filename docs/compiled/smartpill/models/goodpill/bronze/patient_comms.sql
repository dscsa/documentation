
select
cast(jsonb_extract_path_text(_airbyte_data, 'event') as varchar(255)) as event,
cast(jsonb_extract_path_text(_airbyte_data, 'comm_id') as int) as comm_id,
cast(jsonb_extract_path_text(_airbyte_data, 'data_type') as int) as data_type,
cast(jsonb_extract_path_text(_airbyte_data, 'date_sent') as timestamp) as date_sent,
cast(jsonb_extract_path_text(_airbyte_data, 'created_at') as timestamp) as created_at,
cast(jsonb_extract_path_text(_airbyte_data, 'updated_at') as timestamp) as updated_at,
cast(jsonb_extract_path_text(_airbyte_data, 'date_to_send') as timestamp) as date_to_send,
cast(jsonb_extract_path_text(_airbyte_data, 'patient_id_cp') as int) as patient_id_cp,
cast(jsonb_extract_path_text(_airbyte_data, 'invoice_number') as int) as invoice_number,
cast(jsonb_extract_path_text(_airbyte_data, 'group_id') as int) as group_id ,
cast(jsonb_extract_path_text(_airbyte_data, 'rx_number') as int) as rx_number ,
cast(jsonb_extract_path_text(_airbyte_data, 'date_deleted') as timestamp) as date_deleted,
jsonb_extract_path_text(_airbyte_data, 'meta_json')::text as meta_json
from raw."_airbyte_raw_goodpill_gp_patient_comms" arggpc