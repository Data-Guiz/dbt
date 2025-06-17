{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
        fms_spa_hk::CHAR(40) as fms_spa_hk
        ,load_dts::TIMESTAMP as load_dts
        ,record_source::VARCHAR(40) as record_source
        ,dv_run_id::INTEGER as dv_run_id
        ,fms_spa_bk::VARCHAR(40) as fms_spa_bk
    FROM {{ source('dv', 'hub_fms_spa') }}
)

SELECT 
    * 
FROM source_data