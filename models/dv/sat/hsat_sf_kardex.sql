{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
	SELECT
		sf_kardex_hk::CHAR(40) AS sf_kardex_hk
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
		,description__c::VARCHAR(32768) AS description__c
		,bookingbedarrangement__c::VARCHAR(255) AS bookingbedarrangement__c
		,bookingflightclass__c::VARCHAR(255) AS bookingflightclass__c
		,bookingspecificneedsforflight__c::VARCHAR(255) AS bookingspecificneedsforflight__c
		,bookingtypeofstateroomandship__c::VARCHAR(255) AS bookingtypeofstateroomandship__c
		,bookingwheelchairassistance__c::VARCHAR(255) AS bookingwheelchairassistance__c
		,cruiseactivitiespreference__c::VARCHAR(32768) AS cruiseactivitiespreference__c
		,cruisebootsandparkasize__c::VARCHAR(255) AS cruisebootsandparkasize__c
		,cruisefoodandbeverage__c::VARCHAR(1000) AS cruisefoodandbeverage__c
		,cruisehealthinformation__c::VARCHAR(1000) AS cruisehealthinformation__c
		,cruisehousekeeping__c::VARCHAR(32768) AS cruisehousekeeping__c
		,cruiselanguage__c::VARCHAR(255) AS cruiselanguage__c
		,informationcollectedduringbooking__c::VARCHAR(18) AS informationcollectedduringbooking__c
		,triggerdelete__c::VARCHAR(5) AS triggerdelete__c
		,specialrequest__c::VARCHAR(300) AS specialrequest__c
	FROM {{ source('dv', 'hsat_sf_kardex') }}
)
SELECT * FROM source_data