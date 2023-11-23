
select
    user_id as pharmacy_user_id,
    model_type as pharmacy_model_type,
    model_id as pharmacy_model_id,
    type as pharmacy_type,
    reason as pharmacy_reason,
    status as pharmacy_status,
    message as pharmacy_message,
    payload as pharmacy_payload
from "datawarehouse".goodpill."gp_pharmacy_actions"