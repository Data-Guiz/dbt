{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
        kardex_hk::CHAR(40) as kardex_hk
        ,load_dts::TIMESTAMP as load_dts
        ,record_source::VARCHAR(40) as record_source
        ,dv_run_id::INTEGER as dv_run_id
        ,kardex_bk::VARCHAR(40) as kardex_bk
    FROM {{ source('dv', 'hub_kardex') }}
)

SELECT 
    * 
FROM source_data