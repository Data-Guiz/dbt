{{
    config(
        materialized='table',
        schema='dv'
    )
}}
WITH source_data AS (
    SELECT
        sf_household_hk::CHAR(40) as sf_household_hk
        ,load_dts::TIMESTAMP as load_dts
        ,record_source::VARCHAR(40) as record_source
        ,dv_run_id::INTEGER as dv_run_id
        ,sf_household_bk::VARCHAR(80) as sf_household_bk
    FROM {{ source('dv', 'hub_sf_household') }}
)
SELECT 
    * 
FROM source_data    