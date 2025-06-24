{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
        cabin_hk::CHAR(40) as cabin_hk
        ,load_dts::TIMESTAMP as load_dts
        ,record_source::VARCHAR(40) as record_source
        ,dv_run_id::INTEGER as dv_run_id
        ,cabin_bk::VARCHAR(40) as cabin_bk
    FROM {{ source('dv', 'hub_cabin') }}
)

SELECT 
    * 
FROM source_data
