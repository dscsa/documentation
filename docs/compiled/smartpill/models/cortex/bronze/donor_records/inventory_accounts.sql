select
    cast(jsonb_extract_path_text(_airbyte_data, 'id') as bigint) as id,
    cast(jsonb_extract_path_text(_airbyte_data, 'created_at') as timestamp) as created_at,
    cast(jsonb_extract_path_text(_airbyte_data, 'updated_at') as timestamp) as updated_at,
    cast(jsonb_extract_path_text(_airbyte_data, 'recipient_started_at') as timestamp) as recipient_started_at,
    cast(jsonb_extract_path_text(_airbyte_data, 'archived_at') as timestamp) as archived_at,
    cast(jsonb_extract_path_text(_airbyte_data, 'donor_started_at') as timestamp) as donor_started_at,
    cast(jsonb_extract_path_text(_airbyte_data, 'name') as varchar(191)) as name,
    cast(jsonb_extract_path_text(_airbyte_data, 'state') as varchar(191)) as state,
    cast(jsonb_extract_path_text(_airbyte_data, 'county') as varchar(191)) as county,
    cast(jsonb_extract_path_text(_airbyte_data, 'address_1') as varchar(191)) as address_1,
    cast(jsonb_extract_path_text(_airbyte_data, 'address_2') as varchar(191)) as address_2,
    cast(jsonb_extract_path_text(_airbyte_data, 'postal_code') as varchar(191)) as postal_code,
    cast(jsonb_extract_path_text(_airbyte_data, 'phone') as varchar(191)) as phone,
    cast(jsonb_extract_path_text(_airbyte_data, 'pickup_location') as varchar(191)) as pickup_location,
    cast(jsonb_extract_path_text(_airbyte_data, 'instructions') as varchar(191)) as instructions,
    cast(jsonb_extract_path_text(_airbyte_data, 'description') as varchar(191)) as description,
    cast(jsonb_extract_path_text(_airbyte_data, 'fax') as varchar(191)) as fax,
    cast(jsonb_extract_path_text(_airbyte_data, 'license') as varchar(191)) as license,
    cast(jsonb_extract_path_text(_airbyte_data, 'is_donor') as boolean) as is_donor,
    cast(jsonb_extract_path_text(_airbyte_data, 'is_recipient') as boolean) as is_recipient,
    cast(jsonb_extract_path_text(_airbyte_data, 'is_exempt_from_stock_calculations') as boolean) as is_exempt_from_stock_calculations,
    cast(jsonb_extract_path_text(_airbyte_data, 'no_lot_numbers') as boolean) as no_lot_numbers,
    cast(jsonb_extract_path_text(_airbyte_data, 'primary_contact_id') as bigint) as primary_contact_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'default_recipient_account_id') as bigint) as default_recipient_account_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'donation_label_template_id') as bigint) as donation_label_template_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'include_supplies_with_label') as boolean) as include_supplies_with_label,
    cast(jsonb_extract_path_text(_airbyte_data, 'donor_license_type_id') as bigint) as donor_license_type_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'salesforce_id') as varchar(191)) as salesforce_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'require_donation_record') as boolean) as require_donation_record,
    cast(jsonb_extract_path_text(_airbyte_data, 'default_pickup_selection') as boolean) as default_pickup_selection,
    cast(jsonb_extract_path_text(_airbyte_data, 'v2_account_number') as varchar(191)) as v2_account_number,
    cast(jsonb_extract_path_text(_airbyte_data, 'days_per_patient_default') as float) as days_per_patient_default,
    cast(jsonb_extract_path_text(_airbyte_data, 'percent_accepted_default') as float) as percent_accepted_default,
    cast(jsonb_extract_path_text(_airbyte_data, 'percent_dispensed_default') as float) as percent_dispensed_default
from "datawarehouse"."raw"._airbyte_raw_cortex_accounts