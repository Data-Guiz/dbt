{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
	SELECT
		sf_contact_hk::CHAR(40) AS sf_contact_hk
		,load_dts::TIMESTAMP AS load_dts
		,load_end_dts::TIMESTAMP AS load_end_dts
		,del_flag::BOOLEAN AS del_flag
		,hash_diff::VARCHAR(40) AS hash_diff
		,record_source::VARCHAR(40) AS record_source
		,dv_run_id::INTEGER AS dv_run_id
		,isdeleted::VARCHAR(5) AS isdeleted
		,masterrecordid::VARCHAR(18) AS masterrecordid
		,ispersonaccount::VARCHAR(5) AS ispersonaccount
		,lastname::VARCHAR(80) AS lastname
		,firstname::VARCHAR(40) AS firstname
		,salutation::VARCHAR(40) AS salutation
		,name::VARCHAR(121) AS name
		,otherstreet::VARCHAR(255) AS otherstreet
		,othercity::VARCHAR(40) AS othercity
		,otherstate::VARCHAR(80) AS otherstate
		,otherpostalcode::VARCHAR(20) AS otherpostalcode
		,othercountry::VARCHAR(80) AS othercountry
		,otherstatecode::VARCHAR(10) AS otherstatecode
		,othercountrycode::VARCHAR(10) AS othercountrycode
		,mailingstreet::VARCHAR(255) AS mailingstreet
		,mailingcity::VARCHAR(100) AS mailingcity
		,mailingstate::VARCHAR(80) AS mailingstate
		,mailingpostalcode::VARCHAR(20) AS mailingpostalcode
		,mailingcountry::VARCHAR(80) AS mailingcountry
		,mailingstatecode::VARCHAR(10) AS mailingstatecode
		,mailingcountrycode::VARCHAR(10) AS mailingcountrycode
		,phone::VARCHAR(40) AS phone
		,fax::VARCHAR(40) AS fax
		,mobilephone::VARCHAR(40) AS mobilephone
		,homephone::VARCHAR(40) AS homephone
		,otherphone::VARCHAR(40) AS otherphone
		,assistantphone::VARCHAR(40) AS assistantphone
		,reportstoid::VARCHAR(18) AS reportstoid
		,email::VARCHAR(80) AS email
		,title::VARCHAR(128) AS title
		,department::VARCHAR(80) AS department
		,assistantname::VARCHAR(40) AS assistantname
		,leadsource::VARCHAR(40) AS leadsource
		,birthdate::DATE AS birthdate
		,currencyisocode::VARCHAR(3) AS currencyisocode
		,hasoptedoutofemail::VARCHAR(5) AS hasoptedoutofemail
		,createddate::TIMESTAMP AS createddate
		,createdbyid::VARCHAR(18) AS createdbyid
		,lastmodifieddate::TIMESTAMP AS lastmodifieddate
		,lastmodifiedbyid::VARCHAR(18) AS lastmodifiedbyid
		,systemmodstamp::TIMESTAMP AS systemmodstamp
		,lastvieweddate::TIMESTAMP AS lastvieweddate
		,lastreferenceddate::TIMESTAMP AS lastreferenceddate
		,emailbouncedreason::VARCHAR(255) AS emailbouncedreason
		,emailbounceddate::TIMESTAMP AS emailbounceddate
		,photourl::VARCHAR(255) AS photourl
		,jigsaw::VARCHAR(20) AS jigsaw
		,jigsawcontactid::VARCHAR(20) AS jigsawcontactid
		,admiralcardsendingdate__c::DATE AS admiralcardsendingdate__c
		,isprimarycontact__c::VARCHAR(5) AS isprimarycontact__c
		,agreetomailing__c::VARCHAR(5) AS agreetomailing__c
		,agree_to_ng_communications__c::VARCHAR(5) AS agree_to_ng_communications__c
		,airloyaltycardmember__c::VARCHAR(255) AS airloyaltycardmember__c
		,adress_opt_out__c::VARCHAR(5) AS adress_opt_out__c
		,phone_opt_out__c::VARCHAR(5) AS phone_opt_out__c
		,airloyaltycardnumber__c::VARCHAR(100) AS airloyaltycardnumber__c
		,allowduplicate__c::VARCHAR(255) AS allowduplicate__c
		,tech_lastsyncreason__c::VARCHAR(60000) AS tech_lastsyncreason__c
		,type_of_email__c::VARCHAR(255) AS type_of_email__c
		,language__c::VARCHAR(255) AS language__c
		,allow_duplicate__c::VARCHAR(5) AS allow_duplicate__c
		,tech_can_launch_sync__c::VARCHAR(5) AS tech_can_launch_sync__c
		,issuercode__c::VARCHAR(1300) AS issuercode__c
		,pyc_loyalty_status__c::VARCHAR(1300) AS pyc_loyalty_status__c
		,birthdate__c::VARCHAR(40) AS birthdate__c
		,campaignsource__c::VARCHAR(255) AS campaignsource__c
		,commodorecardsendingdate__c::DATE AS commodorecardsendingdate__c
		,tech_last_manual_sync__c::TIMESTAMP AS tech_last_manual_sync__c
		,frequencybrochure__c::VARCHAR(300) AS frequencybrochure__c
		,createddate__c::DATE AS createddate__c
		,contactincaseofemergencyphone__c::VARCHAR(40) AS contactincaseofemergencyphone__c
		,contactincaseofmergencyname__c::VARCHAR(100) AS contactincaseofmergencyname__c
		,cruiser__c::VARCHAR(5) AS cruiser__c
		,dateofadmiralloyaltystatus__c::DATE AS dateofadmiralloyaltystatus__c
		,dateofcommodoreloyaltystatus__c::DATE AS dateofcommodoreloyaltystatus__c
		,dateofgrandadmiralloyaltystatus__c::DATE AS dateofgrandadmiralloyaltystatus__c
		,dateofmajorloyaltystatus__c::DATE AS dateofmajorloyaltystatus__c
		,dateofonetimerloyaltystatus__c::DATE AS dateofonetimerloyaltystatus__c
		,directtrade__c::VARCHAR(255) AS directtrade__c
		,favoritetypeofcommunication__c::VARCHAR(255) AS favoritetypeofcommunication__c
		,gcclient__c::VARCHAR(5) AS gcclient__c
		,gender__c::VARCHAR(255) AS gender__c
		,hasoptedoutoffax::VARCHAR(5) AS hasoptedoutoffax
		,donotcall::VARCHAR(5) AS donotcall
		,cpmergeid__c::VARCHAR(40) AS cpmergeid__c
		,crmmergedid__c::VARCHAR(18) AS crmmergedid__c
		,profile__c::VARCHAR(255) AS profile__c
		,tech_agencycreateddate__c::DATE AS tech_agencycreateddate__c
		,tech_kardexupdated__c::TIMESTAMP AS tech_kardexupdated__c
		,tech_kardex__c::VARCHAR(18) AS tech_kardex__c
		,tech_requestjson__c::VARCHAR(32768) AS tech_requestjson__c
		,tech_syncerrormsg__c::VARCHAR(255) AS tech_syncerrormsg__c
		,tech_syncstatus__c::VARCHAR(255) AS tech_syncstatus__c
		,additionaladdressinformation__c::VARCHAR(255) AS additionaladdressinformation__c
		,repdepartementt__c::VARCHAR(1300) AS repdepartementt__c
		,sansancard__createdbyscantosalesforce__c::VARCHAR(5) AS sansancard__createdbyscantosalesforce__c
		,last_connection_to_ponant_app__c::DATE AS last_connection_to_ponant_app__c
		,id_18_digits__c::VARCHAR(18) AS id_18_digits__c
	FROM {{ source('dv', 'hsat_sf_contact') }}
)
SELECT * FROM source_data
