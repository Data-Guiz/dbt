{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
	SELECT
		sf_event_hk::CHAR(40) AS sf_event_hk
		,load_dts::TIMESTAMP AS load_dts
		,load_end_dts::TIMESTAMP AS load_end_dts
		,del_flag::BOOLEAN AS del_flag
		,hash_diff::VARCHAR(40) AS hash_diff
		,record_source::VARCHAR(40) AS record_source
		,dv_run_id::INTEGER AS dv_run_id
		,recordtypeid::VARCHAR(18) AS recordtypeid
		,whoid::VARCHAR(18) AS whoid
		,whatid::VARCHAR(18) AS whatid
		,whocount::INTEGER AS whocount
		,whatcount::INTEGER AS whatcount
		,subject::VARCHAR(255) AS subject
		,"location"::VARCHAR(255) AS "location"
		,isalldayevent::VARCHAR(5) AS isalldayevent
		,activitydatetime::TIMESTAMP AS activitydatetime
		,activitydate::DATE AS activitydate
		,durationinminutes::INTEGER AS durationinminutes
		,startdatetime::TIMESTAMP AS startdatetime
		,enddatetime::TIMESTAMP AS enddatetime
		,enddate::DATE AS enddate
		,description::VARCHAR(32000) AS description
		,accountid::VARCHAR(18) AS accountid
		,currencyisocode::VARCHAR(3) AS currencyisocode
		,"type"::VARCHAR(40) AS "type"
		,isprivate::VARCHAR(5) AS isprivate
		,showas::VARCHAR(40) AS showas
		,isdeleted::VARCHAR(5) AS isdeleted
		,ischild::VARCHAR(5) AS ischild
		,isgroupevent::VARCHAR(5) AS isgroupevent
		,groupeventtype::VARCHAR(40) AS groupeventtype
		,createddate::TIMESTAMP AS createddate
		,createdbyid::VARCHAR(18) AS createdbyid
		,lastmodifieddate::TIMESTAMP AS lastmodifieddate
		,lastmodifiedbyid::VARCHAR(18) AS lastmodifiedbyid
		,systemmodstamp::TIMESTAMP AS systemmodstamp
		,isarchived::VARCHAR(5) AS isarchived
		,recurrenceactivityid::VARCHAR(18) AS recurrenceactivityid
		,isrecurrence::VARCHAR(5) AS isrecurrence
		,recurrencestartdatetime::TIMESTAMP AS recurrencestartdatetime
		,recurrenceenddateonly::DATE AS recurrenceenddateonly
		,recurrencetimezonesidkey::VARCHAR(40) AS recurrencetimezonesidkey
		,recurrencetype::VARCHAR(40) AS recurrencetype
		,recurrenceinterval::INTEGER AS recurrenceinterval
		,recurrencedayofweekmask::INTEGER AS recurrencedayofweekmask
		,recurrencedayofmonth::INTEGER AS recurrencedayofmonth
		,recurrenceinstance::VARCHAR(40) AS recurrenceinstance
		,recurrencemonthofyear::VARCHAR(40) AS recurrencemonthofyear
		,originalinstancedatetime::TIMESTAMP AS originalinstancedatetime
		,isexception::VARCHAR(5) AS isexception
		,isclientmanaged::VARCHAR(5) AS isclientmanaged
		,clientguid::VARCHAR(255) AS clientguid
		,reminderdatetime::TIMESTAMP AS reminderdatetime
		,isreminderset::VARCHAR(5) AS isreminderset
		,eventsubtype::VARCHAR(40) AS eventsubtype
		,isrecurrence2exclusion::VARCHAR(5) AS isrecurrence2exclusion
		,recurrence2patterntext::VARCHAR(512) AS recurrence2patterntext
		,recurrence2patternversion::VARCHAR(40) AS recurrence2patternversion
		,isrecurrence2::VARCHAR(5) AS isrecurrence2
		,isrecurrence2exception::VARCHAR(5) AS isrecurrence2exception
		,recurrence2patternstartdate::TIMESTAMP AS recurrence2patternstartdate
		,recurrence2patterntimezone::VARCHAR(255) AS recurrence2patterntimezone
		,agencymeeting__c::VARCHAR(5) AS agencymeeting__c
		,brochurename__c::VARCHAR(4099) AS brochurename__c
		,group__c::VARCHAR(5) AS group__c
		,mailinghouse__c::VARCHAR(255) AS mailinghouse__c
		,materialprovided__c::VARCHAR(4099) AS materialprovided__c
		,nameoftheevent__c::VARCHAR(240) AS nameoftheevent__c
		,office__c::VARCHAR(255) AS office__c
		,participantsnumber__c::DOUBLE PRECISION AS participantsnumber__c
		,qcmeeting__c::VARCHAR(5) AS qcmeeting__c
		,reviewmeeting__c::VARCHAR(5) AS reviewmeeting__c
		,status__c::VARCHAR(255) AS status__c
		,strategicmeeting__c::VARCHAR(5) AS strategicmeeting__c
		,tech_requestjson__c::VARCHAR(1000) AS tech_requestjson__c
		,tech_syncerrormsg__c::VARCHAR(255) AS tech_syncerrormsg__c
		,tech_syncstatus__c::VARCHAR(255) AS tech_syncstatus__c
		,tech_contactid__c::VARCHAR(18) AS tech_contactid__c
		,tech_personwhoid__c::VARCHAR(1300) AS tech_personwhoid__c
		,pg__c::VARCHAR(5) AS pg__c
		,externalid__c::VARCHAR(20) AS externalid__c
		,tech_whatid__c::VARCHAR(40) AS tech_whatid__c
		,tech_whoid__c::VARCHAR(40) AS tech_whoid__c
		,tech_updated__c::TIMESTAMP AS tech_updated__c
		,activitydate__c::DATE AS activitydate__c
	FROM {{ source('dv', 'hsat_sf_event') }}
)
SELECT * FROM source_data