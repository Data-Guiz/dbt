{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
	SELECT
		sf_booking_line_item_hk::CHAR(40) AS sf_booking_line_item_hk
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
		,cruise__c::VARCHAR(18) AS cruise__c
		,departurecity__c::VARCHAR(1300) AS departurecity__c
		,externalid__c::VARCHAR(100) AS externalid__c
		,numberofdays__c::DOUBLE PRECISION AS numberofdays__c
		,amount__c::DOUBLE PRECISION AS amount__c
		,bookinglineitemphase__c::VARCHAR(255) AS bookinglineitemphase__c
		,externaldataloadid__c::VARCHAR(1300) AS externaldataloadid__c
		,arrivalcity__c::VARCHAR(1300) AS arrivalcity__c
		,disembarkdate__c::DATE AS disembarkdate__c
		,embarkdate__c::DATE AS embarkdate__c
		,productcode__c::VARCHAR(1300) AS productcode__c
		,productname__c::VARCHAR(1300) AS productname__c
	FROM {{ source('dv', 'hsat_sf_booking_line_item') }}
)
SELECT * FROM source_data