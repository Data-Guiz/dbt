{{
    config(
        materialized='table',
        schema='dv',
        dist='configuration_hk',
        sort=['configuration_hk', 'configuration_bk']
    )
}}

WITH source_data AS (
    SELECT
        configuration_hk::CHAR(40) as configuration_hk
        ,load_dts::TIMESTAMP as load_dts
        ,record_source::VARCHAR(40) as record_source
        ,dv_run_id::INTEGER as dv_run_id
        ,configuration_bk::VARCHAR(40) as configuration_bk
    FROM {{ source('dv', 'hub_configuration') }}
)

SELECT 
    * 
FROM source_data