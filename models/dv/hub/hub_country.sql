{{
    config(
        materialized='table',
        schema='dv',
        dist='country_hk',
        sort=['country_hk', 'country_bk']
    )
}}

WITH source_data AS (
    SELECT
        country_hk::CHAR(40) as country_hk
        ,load_dts::TIMESTAMP as load_dts
        ,record_source::VARCHAR(40) as record_source
        ,dv_run_id::INTEGER as dv_run_id
        ,country_bk::VARCHAR(40) as country_bk
    FROM {{ source('dv', 'hub_country') }}
)

SELECT 
    * 
FROM source_data