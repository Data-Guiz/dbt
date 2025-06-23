{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
	SELECT
		sf_campaignmember_hk::CHAR(40) AS sf_campaignmember_hk
		,load_dts::TIMESTAMP AS load_dts
		,load_end_dts::TIMESTAMP AS load_end_dts
		,del_flag::BOOLEAN AS del_flag
		,hash_diff::VARCHAR(40) AS hash_diff
		,record_source::VARCHAR(40) AS record_source
		,dv_run_id::INTEGER AS dv_run_id
		,isdeleted::VARCHAR(5) AS isdeleted
		,leadid::VARCHAR(18) AS leadid
		,contactid::VARCHAR(18) AS contactid
		,accountid::VARCHAR(18) AS accountid
		,status::VARCHAR(40) AS status
		,hasresponded::VARCHAR(5) AS hasresponded
		,createddate::TIMESTAMP AS createddate
		,createdbyid::VARCHAR(18) AS createdbyid
		,lastmodifieddate::TIMESTAMP AS lastmodifieddate
		,lastmodifiedbyid::VARCHAR(18) AS lastmodifiedbyid
		,systemmodstamp::TIMESTAMP AS systemmodstamp
		,firstrespondeddate::DATE AS firstrespondeddate
		,currencyisocode::VARCHAR(3) AS currencyisocode
		,salutation::VARCHAR(40) AS salutation
		,name::VARCHAR(255) AS name
		,firstname::VARCHAR(40) AS firstname
		,lastname::VARCHAR(80) AS lastname
		,title::VARCHAR(128) AS title
		,street::VARCHAR(255) AS street
		,city::VARCHAR(50) AS city
		,state::VARCHAR(80) AS state
		,postalcode::VARCHAR(20) AS postalcode
		,country::VARCHAR(80) AS country
		,email::VARCHAR(80) AS email
		,phone::VARCHAR(40) AS phone
		,fax::VARCHAR(100) AS fax
		,mobilephone::VARCHAR(40) AS mobilephone
		,description::VARCHAR(32000) AS description
		,donotcall::VARCHAR(5) AS donotcall
		,hasoptedoutofemail::VARCHAR(5) AS hasoptedoutofemail
		,hasoptedoutoffax::VARCHAR(5) AS hasoptedoutoffax
		,leadsource::VARCHAR(40) AS leadsource
		,companyoraccount::VARCHAR(255) AS companyoraccount
		,"type"::VARCHAR(40) AS "type"
		,leadorcontactid::VARCHAR(18) AS leadorcontactid
		,leadorcontactownerid::VARCHAR(18) AS leadorcontactownerid
	FROM {{ source('dv', 'hsat_sf_campaignmember') }}
)
SELECT * FROM source_data