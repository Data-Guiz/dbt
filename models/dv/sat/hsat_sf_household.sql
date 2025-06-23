{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
	SELECT
		sf_household_hk::CHAR(40) AS sf_household_hk
		,load_dts::TIMESTAMP AS load_dts
		,load_end_dts::TIMESTAMP AS load_end_dts
		,del_flag::BOOLEAN AS del_flag
		,hash_diff::VARCHAR(40) AS hash_diff
		,record_source::VARCHAR(40) AS record_source
		,dv_run_id::INTEGER AS dv_run_id
		,isdeleted::VARCHAR(5) AS isdeleted
		,name::VARCHAR(80) AS name
		,currencyisocode::VARCHAR(3) AS currencyisocode
		,createddate::TIMESTAMP AS createddate
		,createdbyid::VARCHAR(18) AS createdbyid
		,lastmodifieddate::TIMESTAMP AS lastmodifieddate
		,lastmodifiedbyid::VARCHAR(18) AS lastmodifiedbyid
		,systemmodstamp::TIMESTAMP AS systemmodstamp
		,lastactivitydate::DATE AS lastactivitydate
		,lastvieweddate::TIMESTAMP AS lastvieweddate
		,lastreferenceddate::TIMESTAMP AS lastreferenceddate
		,household_member__c::VARCHAR(5) AS household_member__c
		,primary__c::VARCHAR(5) AS primary__c
		,reverserelationshipcreated__c::VARCHAR(5) AS reverserelationshipcreated__c
		,person_status__c::VARCHAR(1300) AS person_status__c
		,head_of_household_name__c::VARCHAR(80) AS head_of_household_name__c
	FROM {{ source('dv', 'hsat_sf_household') }}
)
SELECT * FROM source_data