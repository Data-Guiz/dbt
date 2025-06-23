{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
    SELECT
        fms_cruise_hk::VARCHAR(40) AS fms_cruise_hk
        ,load_dts::TIMESTAMP AS load_dts
        ,load_end_dts::TIMESTAMP AS load_end_dts
        ,del_flag::BOOLEAN AS del_flag
        ,hash_diff::VARCHAR(40) AS hash_diff
        ,record_source::VARCHAR(40) AS record_source
        ,dv_run_id::INTEGER AS dv_run_id
        ,shipname::VARCHAR(50) AS shipname
        ,cruise::VARCHAR(100) AS cruise
        ,cruisestart::DATE AS cruisestart
        ,cruiseend::DATE AS cruiseend
        ,curdept::INTEGER AS curdept
        ,curid::VARCHAR(12) AS curid
        ,currency::VARCHAR(50) AS currency
        ,cruisestatus::VARCHAR(6) AS cruisestatus
    FROM {{ source('dv', 'hsat_fms_cruise_setup') }}
)
SELECT * FROM source_data