-- in case of full-refresh need to filter incremental rows from raw source!
with _donors_user as (
	select *,
	ROW_NUMBER() OVER(
		partition by jsonb_extract_path_text(_airbyte_data, 'Id')
		order by "_airbyte_emitted_at" desc
	) as id_row_number
	from "datawarehouse"."raw"._airbyte_raw_salesforce_donors_user
    
        where jsonb_extract_path_text(_airbyte_data, 'LastModifiedDate')::timestamp > (
			select max(user_last_modified_date) from "datawarehouse".salesforce."donors_user"
		)
    
),
donors_user as (

    select
    --ids
    jsonb_extract_path_text(_airbyte_data, 'Id') as user_id,
    --data
    jsonb_extract_path_text(_airbyte_data, 'Fax') as user_fax,
    jsonb_extract_path_text(_airbyte_data, 'City') as user_city,
    jsonb_extract_path_text(_airbyte_data, 'Name') as user_name,
    jsonb_extract_path_text(_airbyte_data, 'Alias') as user_alias,
    jsonb_extract_path_text(_airbyte_data, 'Email') as user_email,
    jsonb_extract_path_text(_airbyte_data, 'Phone') as user_phone,
    jsonb_extract_path_text(_airbyte_data, 'State') as user_state,
    jsonb_extract_path_text(_airbyte_data, 'Title') as user_title,
    jsonb_extract_path_text(_airbyte_data, 'Street') as user_street,
    jsonb_extract_path_text(_airbyte_data, 'AboutMe') as user_about_me,

    _airbyte_data -> 'Address' -> 'city' as city,
    _airbyte_data -> 'Address' -> 'state' as state,
    _airbyte_data -> 'Address' -> 'street' as street,
    _airbyte_data -> 'Address' -> 'country' as country,
    _airbyte_data -> 'Address' -> 'latitude' as latitude,
    _airbyte_data -> 'Address' -> 'longitude' as longitude,
    _airbyte_data -> 'Address' -> 'postalCode' as postal_code,
    _airbyte_data -> 'Address' -> 'geocodeAccuracy' as geocode_accuracy,

    jsonb_extract_path_text(_airbyte_data, 'Country') as user_country,
    jsonb_extract_path_text(_airbyte_data, 'LastName') as user_lastname,
    jsonb_extract_path_text(_airbyte_data, 'UserType') as user_usertype,
    jsonb_extract_path_text(_airbyte_data, 'Username') as user_username,
    jsonb_extract_path_text(_airbyte_data, 'FirstName') as user_firstname,
    jsonb_extract_path_text(_airbyte_data, 'PostalCode') as user_postal_code,
    cast(jsonb_extract_path_text(_airbyte_data, 'LastModifiedDate') as timestamp) as user_last_modified_date,
    jsonb_extract_path_text(_airbyte_data, 'IsDeleted')::bool as user_isdeleted,
    _airbyte_emitted_at as user_airbyte_emitted_at
    from _donors_user
    where id_row_number = 1
)
select distinct * from donors_user