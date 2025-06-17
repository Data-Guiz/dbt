{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
        mr_vessel_type_hk::CHAR(40) as mr_vessel_type_hk
        ,load_dts::TIMESTAMP as load_dts
        ,record_source::VARCHAR(40) as record_source
        ,dv_run_id::INTEGER as dv_run_id
        ,vessel_type_id::VARCHAR(40) as vessel_type_id
    FROM {{ source('dv', 'hub_mr_vessel_type') }}
)

SELECT 
    * 
FROM source_data
