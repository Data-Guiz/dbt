{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
	SELECT
		sf_account_hk::CHAR(40) AS sf_account_hk
		,load_dts::TIMESTAMP AS load_dts
		,load_end_dts::TIMESTAMP AS load_end_dts
		,del_flag::BOOLEAN AS del_flag
		,hash_diff::VARCHAR(40) AS hash_diff
		,record_source::VARCHAR(40) AS record_source
		,dv_run_id::INTEGER AS dv_run_id
		,systemmodstamp::TIMESTAMP AS systemmodstamp
		,"type"::VARCHAR(30) AS "type"
		,typologie__c::VARCHAR(30) AS typologie__c
		,is_an_active_client__c::VARCHAR(10) AS is_an_active_client__c
		,segmentation__c::VARCHAR(100) AS segmentation__c
		,heatscore__c::VARCHAR(25) AS heatscore__c
	FROM {{ source('dv', 'hsat_sf_account_staging') }}
)
SELECT * FROM source_data