{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
        sf_event_hk::CHAR(40) as sf_event_hk
        ,load_dts::TIMESTAMP as load_dts
        ,record_source::VARCHAR(40) as record_source
        ,dv_run_id::INTEGER as dv_run_id
        ,sf_event_bk::VARCHAR(40) as sf_event_bk
    FROM {{ source('dv', 'hub_sf_event') }} 
)
SELECT 
    * 
FROM source_data