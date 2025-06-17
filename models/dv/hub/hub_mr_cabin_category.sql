{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
        mr_cabin_category_hk::CHAR(40) as mr_cabin_category_hk
        ,load_dts::TIMESTAMP as load_dts
        ,record_source::VARCHAR(40) as record_source
        ,dv_run_id::INTEGER as dv_run_id
        ,cabin_category_id::VARCHAR(40) as cabin_category_id
    FROM {{ source('dv', 'hub_mr_cabin_category') }}
)

SELECT 
    * 
FROM source_data
