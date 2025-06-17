{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
        mm_cruise_hk::CHAR(40) as mm_cruise_hk
        ,load_dts::TIMESTAMP as load_dts
        ,record_source::VARCHAR(40) as record_source
        ,dv_run_id::INTEGER as dv_run_id
        ,mm_cruise_bk::VARCHAR(40) as mm_cruise_bk
    FROM {{ source('dv', 'hub_mm_cruise') }}
)

SELECT 
    * 
FROM source_data