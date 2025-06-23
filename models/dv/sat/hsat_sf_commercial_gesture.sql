{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
	SELECT
		sf_commercial_gesture_hk::CHAR(40) AS sf_commercial_gesture_hk
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
		,appliedonbooking__c::VARCHAR(18) AS appliedonbooking__c
		,tech_transcoid__c::VARCHAR(1300) AS tech_transcoid__c
		,case__c::VARCHAR(18) AS case__c
		,repcruise__c::VARCHAR(1300) AS repcruise__c
		,approved__c::VARCHAR(5) AS approved__c
		,beginningofthevalidity__c::DATE AS beginningofthevalidity__c
		,comments__c::VARCHAR(32768) AS comments__c
		,dateofcredituse__c::DATE AS dateofcredituse__c
		,endofvalidity__c::DATE AS endofvalidity__c
		,externalid__c::VARCHAR(40) AS externalid__c
		,goodwillgestureitem__c::VARCHAR(255) AS goodwillgestureitem__c
		,goodwillgesturestatus__c::VARCHAR(255) AS goodwillgesturestatus__c
		,goodwillgesturevalue__c::DOUBLE PRECISION AS goodwillgesturevalue__c
		,origin__c::VARCHAR(255) AS origin__c
		,tech_requestjson__c::VARCHAR(32768) AS tech_requestjson__c
		,tech_syncerrormsg__c::VARCHAR(255) AS tech_syncerrormsg__c
		,tech_syncstatus__c::VARCHAR(255) AS tech_syncstatus__c
	FROM {{ source('dv', 'hsat_sf_commercial_gesture') }}
)
SELECT * FROM source_data