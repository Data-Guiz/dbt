{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
    SELECT
        group_hk::VARCHAR(40) AS group_hk
        ,load_dts::TIMESTAMP AS load_dts
        ,load_end_dts::TIMESTAMP AS load_end_dts
        ,del_flag::BOOLEAN AS del_flag
        ,hash_diff::VARCHAR(40) AS hash_diff
        ,record_source::VARCHAR(40) AS record_source
        ,dv_run_id::INTEGER AS dv_run_id
        ,file_name::VARCHAR(100) AS file_name
        ,datetime_from_file_name::TIMESTAMP AS datetime_from_file_name
        ,contractid::VARCHAR(10) AS contractid
        ,groupname::VARCHAR(100) AS groupname
        ,grouptype::VARCHAR(10) AS grouptype
        ,groupbkddate::DATE AS groupbkddate
        ,groupstatus::VARCHAR(100) AS groupstatus
        ,groupoffice::VARCHAR(100) AS groupoffice
        ,groupmarket::VARCHAR(100) AS groupmarket
        ,groupcurrency::VARCHAR(10) AS groupcurrency
        ,groupagencyid::VARCHAR(10) AS groupagencyid
        ,groupagencyname::VARCHAR(100) AS groupagencyname
        ,cruiselength::INTEGER AS cruiselength
        ,charter::VARCHAR(10) AS charter
        ,departureport::VARCHAR(10) AS departureport
        ,arrivalport::VARCHAR(10) AS arrivalport
        ,departuredate::DATE AS departuredate
        ,arrivaldate::DATE AS arrivaldate
        ,contractedcabins::INTEGER AS contractedcabins
        ,contractedbeds::INTEGER AS contractedbeds
        ,contractedpax::INTEGER AS contractedpax
        ,depositamount::VARCHAR(20) AS depositamount
        ,depositamount__number::DOUBLE PRECISION AS depositamount__number
        ,paxamount::VARCHAR(20) AS paxamount
        ,paxamount__number::DOUBLE PRECISION AS paxamount__number
        ,contractedamount::VARCHAR(20) AS contractedamount
        ,contractedamount__number::DOUBLE PRECISION AS contractedamount__number
        ,soldcabins::INTEGER AS soldcabins
        ,soldpax::VARCHAR(20) AS soldpax
        ,soldpax__number::DOUBLE PRECISION AS soldpax__number
        ,soldamount::VARCHAR(20) AS soldamount
        ,soldamount__number::DOUBLE PRECISION AS soldamount__number
        ,percentrate::VARCHAR(20) AS percentrate
        ,percentrate__number::DOUBLE PRECISION AS percentrate__number
        ,inporgress::VARCHAR(20) AS inporgress
        ,inporgress__number::DOUBLE PRECISION AS inporgress__number
        ,releasecabin::INTEGER AS releasecabin
        ,releasefees::VARCHAR(20) AS releasefees
        ,releasefees__number::DOUBLE PRECISION AS releasefees__number
        ,cruiseid::VARCHAR(10) AS cruiseid
    FROM {{ source('dv', 'hsat_cp_groupcontractinfo') }}
)
SELECT * FROM source_data
