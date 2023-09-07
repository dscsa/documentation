-- in case of full-refresh need to filter incremental rows from raw source!
with _contact as (
	select *, ROW_NUMBER() OVER(
		partition by jsonb_extract_path_text(_airbyte_data, 'Id')
		order by "_airbyte_emitted_at" desc
	) as id_row_number
	from "datawarehouse"."raw"._airbyte_raw_salesforce_contact
	
	where jsonb_extract_path_text(_airbyte_data, 'LastModifiedDate')::timestamp > (
		select max(contact_last_modified_date) from "datawarehouse".salesforce."patients_contact"
	)
    
),
contact as (
    select
	-- ids
	jsonb_extract_path_text(_airbyte_data, 'Id') as contact_id,
	jsonb_extract_path_text(_airbyte_data, 'OwnerId') as contact_owner_id,
	jsonb_extract_path_text(_airbyte_data, 'gp_patient_id_cp__c')::float::int as contact_gp_patient_id_cp__c,
	jsonb_extract_path_text(_airbyte_data, 'gp_patient_id_wc__c')::float::int as contact_gp_patient_id_wc__c,
	-- data
	jsonb_extract_path_text(_airbyte_data, 'Name') as contact_name,
	jsonb_extract_path_text(_airbyte_data, 'Birthdate') as contact_birthdate,
	jsonb_extract_path_text(_airbyte_data, 'LastName') as contact_lastname,
	jsonb_extract_path_text(_airbyte_data, 'FirstName') as contact_firstname,
	jsonb_extract_path_text(_airbyte_data, 'CreatedDate')::timestamp as contact_created_date,
	jsonb_extract_path_text(_airbyte_data, 'Email') as contact_email,
	jsonb_extract_path_text(_airbyte_data, 'Phone') as contact_phone,
	jsonb_extract_path_text(_airbyte_data, 'Title') as contact_title,
	jsonb_extract_path_text(_airbyte_data, 'gp_payment_card_type__c') as contact_gp_payment_card_type__c,
	jsonb_extract_path_text(_airbyte_data, 'Description') as contact_description,
	cast(jsonb_extract_path_text(_airbyte_data, 'LastModifiedDate') as timestamp) as contact_last_modified_date,
	jsonb_extract_path_text(_airbyte_data, 'AccountId') as contact_account_id,
	jsonb_extract_path_text(_airbyte_data, 'gp_language__c') as contact_gp_language__c,
	jsonb_extract_path_text(_airbyte_data, 'gp_patient_address1__c') as contact_gp_patient_address1__c,
	jsonb_extract_path_text(_airbyte_data, 'gp_patient_address2__c') as contact_gp_patient_address2__c,
	jsonb_extract_path_text(_airbyte_data, 'gp_patient_autofill__c') as contact_gp_patient_autofill__c,

	jsonb_extract_path_text(_airbyte_data, 'gp_patient_city__c') as contact_gp_patient_city__c,
	cast(jsonb_extract_path_text(_airbyte_data, 'gp_patient_date_added__c') as timestamp) as contact_gp_patient_date_added__c,
	cast(jsonb_extract_path_text(_airbyte_data, 'gp_patient_date_changed__c') as timestamp) as contact_gp_patient_date_changed__c,
	jsonb_extract_path_text(_airbyte_data, 'gp_patient_note__c') as contact_gp_patient_note__c,
	jsonb_extract_path_text(_airbyte_data, 'gp_patient_state__c') as contact_gp_patient_state__c,
	jsonb_extract_path_text(_airbyte_data, 'gp_patient_status__c') as contact_gp_patient_status__c,
	jsonb_extract_path_text(_airbyte_data, 'gp_patient_zip__c') as contact_gp_patient_zip__c,

	cast(jsonb_extract_path_text(_airbyte_data, 'gp_payment_card_date_expired__c') as timestamp) as contact_gp_payment_card_date_expired__c,
	jsonb_extract_path_text(_airbyte_data, 'gp_payment_card_last4__c') as contact_gp_payment_card_last4__c,

	jsonb_extract_path_text(_airbyte_data, 'gp_payment_coupon__c') as contact_gp_payment_coupon__c,
	jsonb_extract_path_text(_airbyte_data, 'gp_payment_method__c') as contact_gp_payment_method__c,

	jsonb_extract_path_text(_airbyte_data, 'gp_pharmacy_fax__c') as contact_gp_pharmacy_fax__c,
	jsonb_extract_path_text(_airbyte_data, 'gp_pharmacy_npi__c') as contact_gp_pharmacy_npi__c,
	jsonb_extract_path_text(_airbyte_data, 'gp_pharmacy_name__c') as contact_gp_pharmacy_name__c,
	jsonb_extract_path_text(_airbyte_data, 'gp_pharmacy_phone__c') as contact_gp_pharmacy_phone__c,
	jsonb_extract_path_text(_airbyte_data, 'gp_pharmacy_address__c') as contact_gp_pharmacy_address__c,

	jsonb_extract_path_text(_airbyte_data, 'gp_phone1__c') as contact_gp_phone1__c,
	jsonb_extract_path_text(_airbyte_data, 'gp_phone2__c') as contact_gp_phone2__c,

	jsonb_extract_path_text(_airbyte_data, 'gp_refills_used__c') as contact_gp_refills_used__c,
	jsonb_extract_path_text(_airbyte_data, 'gp_tracking_coupon__c') as contact_gp_tracking_coupon__c,
	jsonb_extract_path_text(_airbyte_data, 'RecordTypeId') as contact_recordtype_id,
	jsonb_extract_path_text(_airbyte_data, 'first_name__c') as contact_first_name__c,
	jsonb_extract_path_text(_airbyte_data, 'gp_email__c') as contact_gp_email__c,
	jsonb_extract_path_text(_airbyte_data, 'IsDeleted')::bool as contact_isdeleted,
	_airbyte_emitted_at as contact_airbyte_emitted_at

	from _contact
	where id_row_number = 1
)
select * from contact