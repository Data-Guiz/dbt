{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
	SELECT
		sf_campaign_hk::CHAR(40) AS sf_campaign_hk
		,load_dts::TIMESTAMP AS load_dts
		,load_end_dts::TIMESTAMP AS load_end_dts
		,del_flag::BOOLEAN AS del_flag
		,hash_diff::VARCHAR(40) AS hash_diff
		,record_source::VARCHAR(40) AS record_source
		,dv_run_id::INTEGER AS dv_run_id
		,isdeleted::VARCHAR(5) AS isdeleted
		,parentid::VARCHAR(18) AS parentid
		,"type"::VARCHAR(40) AS "type"
		,status::VARCHAR(40) AS status
		,startdate::DATE AS startdate
		,enddate::DATE AS enddate
		,currencyisocode::VARCHAR(3) AS currencyisocode
		,expectedrevenue::DOUBLE PRECISION AS expectedrevenue
		,budgetedcost::DOUBLE PRECISION AS budgetedcost
		,actualcost::DOUBLE PRECISION AS actualcost
		,expectedresponse::DOUBLE PRECISION AS expectedresponse
		,numbersent::DOUBLE PRECISION AS numbersent
		,isactive::VARCHAR(5) AS isactive
		,description::VARCHAR(32000) AS description
		,numberofleads::INTEGER AS numberofleads
		,numberofconvertedleads::INTEGER AS numberofconvertedleads
		,numberofcontacts::INTEGER AS numberofcontacts
		,numberofresponses::INTEGER AS numberofresponses
		,numberofopportunities::INTEGER AS numberofopportunities
		,numberofwonopportunities::INTEGER AS numberofwonopportunities
		,amountallopportunities::DOUBLE PRECISION AS amountallopportunities
		,amountwonopportunities::DOUBLE PRECISION AS amountwonopportunities
		,hierarchynumberofleads::INTEGER AS hierarchynumberofleads
		,hierarchynumberofconvertedleads::INTEGER AS hierarchynumberofconvertedleads
		,hierarchynumberofcontacts::INTEGER AS hierarchynumberofcontacts
		,hierarchynumberofresponses::INTEGER AS hierarchynumberofresponses
		,hierarchynumberofopportunities::INTEGER AS hierarchynumberofopportunities
		,hierarchynumberofwonopportunities::INTEGER AS hierarchynumberofwonopportunities
		,hierarchyamountallopportunities::DOUBLE PRECISION AS hierarchyamountallopportunities
		,hierarchyamountwonopportunities::DOUBLE PRECISION AS hierarchyamountwonopportunities
		,hierarchynumbersent::DOUBLE PRECISION AS hierarchynumbersent
		,hierarchyexpectedrevenue::DOUBLE PRECISION AS hierarchyexpectedrevenue
		,hierarchybudgetedcost::DOUBLE PRECISION AS hierarchybudgetedcost
		,hierarchyactualcost::DOUBLE PRECISION AS hierarchyactualcost
		,createddate::TIMESTAMP AS createddate
		,createdbyid::VARCHAR(18) AS createdbyid
		,lastmodifieddate::TIMESTAMP AS lastmodifieddate
		,lastmodifiedbyid::VARCHAR(18) AS lastmodifiedbyid
		,systemmodstamp::TIMESTAMP AS systemmodstamp
		,lastactivitydate::DATE AS lastactivitydate
		,lastvieweddate::TIMESTAMP AS lastvieweddate
		,lastreferenceddate::TIMESTAMP AS lastreferenceddate
		,campaignmemberrecordtypeid::VARCHAR(18) AS campaignmemberrecordtypeid
		,iscampaignsource__c::VARCHAR(5) AS iscampaignsource__c
		,office__c::VARCHAR(255) AS office__c
		,target__c::VARCHAR(255) AS target__c
		,testaccountincampaign__c::DOUBLE PRECISION AS testaccountincampaign__c
		,nbaccountb2b__c::DOUBLE PRECISION AS nbaccountb2b__c
		,nbb2cincampaign__c::DOUBLE PRECISION AS nbb2cincampaign__c
		,promotioncode__c::VARCHAR(100) AS promotioncode__c
	FROM {{ source('dv', 'hsat_sf_campaign') }}
)
SELECT * FROM source_data