{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
	SELECT
		sf_case_hk::CHAR(40) AS sf_case_hk
		,load_dts::TIMESTAMP AS load_dts
		,load_end_dts::TIMESTAMP AS load_end_dts
		,del_flag::BOOLEAN AS del_flag
		,hash_diff::VARCHAR(40) AS hash_diff
		,record_source::VARCHAR(40) AS record_source
		,dv_run_id::INTEGER AS dv_run_id
		,isdeleted::VARCHAR(5) AS isdeleted
		,masterrecordid::VARCHAR(18) AS masterrecordid
		,casenumber::VARCHAR(30) AS casenumber
		,contactid::VARCHAR(18) AS contactid
		,accountid::VARCHAR(18) AS accountid
		,sourceid::VARCHAR(18) AS sourceid
		,parentid::VARCHAR(18) AS parentid
		,suppliedname::VARCHAR(80) AS suppliedname
		,suppliedemail::VARCHAR(80) AS suppliedemail
		,suppliedphone::VARCHAR(40) AS suppliedphone
		,suppliedcompany::VARCHAR(80) AS suppliedcompany
		,"type"::VARCHAR(40) AS "type"
		,status::VARCHAR(40) AS status
		,reason::VARCHAR(40) AS reason
		,origin::VARCHAR(40) AS origin
		,"language"::VARCHAR(40) AS "language"
		,subject::VARCHAR(300) AS subject
		,"priority"::VARCHAR(40) AS "priority"
		,description::VARCHAR(32000) AS description
		,isclosed::VARCHAR(5) AS isclosed
		,closeddate::TIMESTAMP AS closeddate
		,isescalated::VARCHAR(5) AS isescalated
		,currencyisocode::VARCHAR(3) AS currencyisocode
		,createddate::TIMESTAMP AS createddate
		,createdbyid::VARCHAR(18) AS createdbyid
		,lastmodifieddate::TIMESTAMP AS lastmodifieddate
		,lastmodifiedbyid::VARCHAR(18) AS lastmodifiedbyid
		,systemmodstamp::TIMESTAMP AS systemmodstamp
		,contactphone::VARCHAR(40) AS contactphone
		,contactmobile::VARCHAR(40) AS contactmobile
		,contactemail::VARCHAR(80) AS contactemail
		,contactfax::VARCHAR(40) AS contactfax
		,comments::VARCHAR(4000) AS comments
		,lastvieweddate::TIMESTAMP AS lastvieweddate
		,lastreferenceddate::TIMESTAMP AS lastreferenceddate
		,case_age__c::DOUBLE PRECISION AS case_age__c
		,booking__c::VARCHAR(18) AS booking__c
		,alacarteservice__c::VARCHAR(5) AS alacarteservice__c
		,airlineflights__c::VARCHAR(5) AS airlineflights__c
		,capacity__c::VARCHAR(150) AS capacity__c
		,commercialgesturecreatedreferral__c::VARCHAR(5) AS commercialgesturecreatedreferral__c
		,createddate__c::TIMESTAMP AS createddate__c
		,cruise__c::VARCHAR(5) AS cruise__c
		,destinations__c::VARCHAR(150) AS destinations__c
		,entertainment__c::VARCHAR(5) AS entertainment__c
		,excursions__c::VARCHAR(5) AS excursions__c
		,expeditions__c::VARCHAR(5) AS expeditions__c
		,externalid__c::VARCHAR(100) AS externalid__c
		,flights__c::VARCHAR(5) AS flights__c
		,food__c::VARCHAR(5) AS food__c
		,issuefor__c::VARCHAR(255) AS issuefor__c
		,medevac__c::VARCHAR(5) AS medevac__c
		,mass_close__c::VARCHAR(5) AS mass_close__c
		,multimedia__c::VARCHAR(5) AS multimedia__c
		,ocodid__c::VARCHAR(20) AS ocodid__c
		,office__c::VARCHAR(255) AS office__c
		,onboardinvoices__c::VARCHAR(5) AS onboardinvoices__c
		,ponantflights__c::VARCHAR(5) AS ponantflights__c
		,ponantstudio__c::VARCHAR(5) AS ponantstudio__c
		,preandpostprogramme__c::VARCHAR(5) AS preandpostprogramme__c
		,prepost__c::VARCHAR(5) AS prepost__c
		,pricemisunderstandingspecialoffer__c::VARCHAR(5) AS pricemisunderstandingspecialoffer__c
		,reasonofnonanswer__c::VARCHAR(140) AS reasonofnonanswer__c
		,referralstatus__c::VARCHAR(255) AS referralstatus__c
		,reopenedreason__c::VARCHAR(255) AS reopenedreason__c
		,reopened__c::VARCHAR(5) AS reopened__c
		,reservationservice__c::VARCHAR(5) AS reservationservice__c
		,shipcomfort__c::VARCHAR(5) AS shipcomfort__c
		,sponsor__c::VARCHAR(18) AS sponsor__c
		,sponsored__c::VARCHAR(18) AS sponsored__c
		,staff__c::VARCHAR(5) AS staff__c
		,tech_requestjson__c::VARCHAR(32768) AS tech_requestjson__c
		,tech_syncerrormsg__c::VARCHAR(255) AS tech_syncerrormsg__c
		,tech_syncstatus__c::VARCHAR(255) AS tech_syncstatus__c
		,transfer__c::VARCHAR(5) AS transfer__c
		,year__c::VARCHAR(4) AS year__c
		,tech_iscontactnameempty__c::VARCHAR(5) AS tech_iscontactnameempty__c
		,techownerid__c::VARCHAR(1300) AS techownerid__c
		,istypeclaim__c::VARCHAR(5) AS istypeclaim__c
		,laststatuschange__c::DATE AS laststatuschange__c
		,noofdayssincechange__c::DOUBLE PRECISION AS noofdayssincechange__c
		,unchange21days__c::VARCHAR(5) AS unchange21days__c
		,createdlast24hr__c::VARCHAR(5) AS createdlast24hr__c
		,opened24hr__c::VARCHAR(5) AS opened24hr__c
		,repcruise__c::VARCHAR(1300) AS repcruise__c
		,old_case_id_del__c::VARCHAR(18) AS old_case_id_del__c
	FROM {{ source('dv', 'hsat_sf_case') }}
)
SELECT * FROM source_data
