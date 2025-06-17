{{
    config(
        materialized='table',
        schema='dv',
        dist='discount_hk',
        sort=['discount_hk', 'discount_bk']
    )
}}

WITH source_data AS (
    SELECT
        discount_hk::CHAR(40) as discount_hk
        ,load_dts::TIMESTAMP as load_dts
        ,record_source::VARCHAR(40) as record_source
        ,dv_run_id::INTEGER as dv_run_id
        ,discount_bk::VARCHAR(40) as discount_bk
    FROM {{ source('dv', 'hub_discount') }}
)

SELECT 
    * 
FROM source_data