-- in case of full-refresh need to filter incremental rows from raw source!
with _gp_user as (
	select *,
	ROW_NUMBER() OVER(
		partition by jsonb_extract_path_text(_airbyte_data, 'Id')
		order by "_airbyte_emitted_at" desc
	) as id_row_number
	from "datawarehouse"."raw"._airbyte_raw_salesforce_gp_user__c
	
	where jsonb_extract_path_text(_airbyte_data, 'LastModifiedDate')::timestamp > (
		select max(lastmodifieddate) from "datawarehouse".salesforce."patients_gp_user__c"
	)
    
),
gp_user as (
    select
	jsonb_extract_path_text(_airbyte_data, 'Id') as Id,
	jsonb_extract_path_text(_airbyte_data, 'Name') as Name,
	jsonb_extract_path_text(_airbyte_data, 'OwnerId') as OwnerId,
	jsonb_extract_path_text(_airbyte_data, 'Role__c') as Role__c,
	jsonb_extract_path_text(_airbyte_data, 'Email__c') as Email__c,
	jsonb_extract_path_text(_airbyte_data, 'IsDeleted')::bool as IsDeleted,
	jsonb_extract_path_text(_airbyte_data, 'Account__c') as Account__c,
	jsonb_extract_path_text(_airbyte_data, 'Archived__c') as Archived__c,
	jsonb_extract_path_text(_airbyte_data, 'CreatedById') as CreatedById,
	jsonb_extract_path_text(_airbyte_data, 'CreatedDate') as CreatedDate,
	jsonb_extract_path_text(_airbyte_data, 'Is_Human__c') as Is_Human__c,
	jsonb_extract_path_text(_airbyte_data, 'RecordTypeId') as RecordTypeId,
	jsonb_extract_path_text(_airbyte_data, 'LastViewedDate') as LastViewedDate,
	jsonb_extract_path_text(_airbyte_data, 'SystemModstamp') as SystemModstamp,
	jsonb_extract_path_text(_airbyte_data, 'LastActivityDate') as LastActivityDate,
	jsonb_extract_path_text(_airbyte_data, 'LastModifiedById') as LastModifiedById,
	cast(jsonb_extract_path_text(_airbyte_data, 'LastModifiedDate') as timestamp) as LastModifiedDate,
	jsonb_extract_path_text(_airbyte_data, 'LastReferencedDate') as LastReferencedDate,
	_airbyte_emitted_at as gp_user_airbyte_emitted_at
	from _gp_user
	where id_row_number = 1
)
select * from gp_user