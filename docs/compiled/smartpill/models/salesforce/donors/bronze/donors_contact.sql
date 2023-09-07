-- in case of full-refresh need to filter incremental rows from raw source!
with _contact as (
	select *,
	ROW_NUMBER() OVER(
		partition by jsonb_extract_path_text(_airbyte_data, 'Id')
		order by "_airbyte_emitted_at" desc
	) as id_row_number
	from "datawarehouse"."raw"._airbyte_raw_salesforce_donors_contact
	
        where jsonb_extract_path_text(_airbyte_data, 'LastModifiedDate')::timestamp > (
			select max(contact_last_modified_date) from "datawarehouse".salesforce."donors_contact"
		)
    
),
contact as (
    select
	-- ids
	jsonb_extract_path_text(_airbyte_data, 'Id') as contact_id,
	jsonb_extract_path_text(_airbyte_data, 'OwnerId') as contact_owner_id,
	-- data
	jsonb_extract_path_text(_airbyte_data, 'Name') as contact_name,
	jsonb_extract_path_text(_airbyte_data, 'Birthdate') as contact_birthdate,
	jsonb_extract_path_text(_airbyte_data, 'LastName') as contact_lastname,
	jsonb_extract_path_text(_airbyte_data, 'FirstName') as contact_firstname,
	jsonb_extract_path_text(_airbyte_data, 'Email') as contact_email,
	jsonb_extract_path_text(_airbyte_data, 'Phone') as contact_phone,
	jsonb_extract_path_text(_airbyte_data, 'Title') as contact_title,
	jsonb_extract_path_text(_airbyte_data, 'Description') as contact_description,
	cast(jsonb_extract_path_text(_airbyte_data, 'LastModifiedDate') as timestamp) as contact_last_modified_date,
	jsonb_extract_path_text(_airbyte_data, 'IsDeleted')::bool as contact_isdeleted,
	_airbyte_emitted_at as contact_airbyte_emitted_at
	from _contact
	where id_row_number = 1
)
select * from contact