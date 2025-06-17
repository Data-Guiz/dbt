{{
    config(
        materialized='table',
        schema='dv',
        dist='cabin_category_hk',
        sort=['cabin_category_hk', 'cabin_category_bk']
    )
}}

WITH source_data AS (
    SELECT
        cabin_category_hk::CHAR(40) as cabin_category_hk
        ,load_dts::TIMESTAMP as load_dts
        ,record_source::VARCHAR(40) as record_source
        ,dv_run_id::INTEGER as dv_run_id
        ,cabin_category_bk::VARCHAR(40) as cabin_category_bk
    FROM {{ source('dv', 'hub_cabin_category') }}
)

SELECT 
    * 
FROM source_data
