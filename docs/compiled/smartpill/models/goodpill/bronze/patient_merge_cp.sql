select
cast(jsonb_extract_path_text(_airbyte_data, 'id') as bigint) as id,
cast(jsonb_extract_path_text(_airbyte_data, 'source_patient_id_cp') as int) as source_patient_id_cp,
cast(jsonb_extract_path_text(_airbyte_data, 'target_patient_id_cp') as int) as target_patient_id_cp,
cast(jsonb_extract_path_text(_airbyte_data, 'merged_at') as timestamp) as merged_at
from "datawarehouse"."raw"._airbyte_raw_goodpill_gp_patient_merge_cp