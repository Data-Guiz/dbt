{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
        sf_account_hk::CHAR(40) AS sf_account_hk,
        load_dts::TIMESTAMP AS load_dts,
        load_end_dts::TIMESTAMP AS load_end_dts,
        del_flag::BOOLEAN AS del_flag,
        hash_diff::VARCHAR(40) AS hash_diff,
        record_source::VARCHAR(40) AS record_source,
        dv_run_id::INTEGER AS dv_run_id,
        isdeleted::VARCHAR(5) AS isdeleted,
        masterrecordid::VARCHAR(18) AS masterrecordid,
        name::VARCHAR(255) AS name,
        lastname::VARCHAR(80) AS lastname,
        firstname::VARCHAR(40) AS firstname,
        salutation::VARCHAR(40) AS salutation,
        type::VARCHAR(40) AS type,
        recordtypeid::VARCHAR(18) AS recordtypeid,
        parentid::VARCHAR(18) AS parentid,
        billingstreet::VARCHAR(255) AS billingstreet,
        billingcity::VARCHAR(60) AS billingcity,
        billingstate::VARCHAR(80) AS billingstate,
        billingpostalcode::VARCHAR(20) AS billingpostalcode,
        billingcountry::VARCHAR(80) AS billingcountry,
        billingstatecode::VARCHAR(10) AS billingstatecode,
        billingcountrycode::VARCHAR(10) AS billingcountrycode,
        shippingstreet::VARCHAR(255) AS shippingstreet,
        shippingcity::VARCHAR(100) AS shippingcity,
        shippingstate::VARCHAR(80) AS shippingstate,
        shippingpostalcode::VARCHAR(20) AS shippingpostalcode,
        shippingcountry::VARCHAR(80) AS shippingcountry,
        shippingstatecode::VARCHAR(10) AS shippingstatecode,
        shippingcountrycode::VARCHAR(10) AS shippingcountrycode,
        phone::VARCHAR(40) AS phone,
        fax::VARCHAR(60) AS fax,
        website::VARCHAR(255) AS website,
        photourl::VARCHAR(255) AS photourl,
        industry::VARCHAR(40) AS industry,
        numberofemployees::INTEGER AS numberofemployees,
        ownership::VARCHAR(40) AS ownership,
        site::VARCHAR(80) AS site,
        currencyisocode::VARCHAR(3) AS currencyisocode,
        createddate::TIMESTAMP AS createddate,
        createdbyid::VARCHAR(18) AS createdbyid
    FROM {{ source('dv', 'hsat_sf_account') }}
)

SELECT * FROM source_data 