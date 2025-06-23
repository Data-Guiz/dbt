{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
        configuration__mr_vessel__mr_vessel_configuration_hk::CHAR(40) as configuration__mr_vessel__mr_vessel_configuration_hk,
        load_dts::TIMESTAMP as load_dts,
        record_src::VARCHAR(50) as record_src,
        dv_runid::INTEGER as dv_runid,
        configuration_hk::CHAR(40) as configuration_hk,
        mr_vessel_configuration_hk::CHAR(40) as mr_vessel_configuration_hk,
        mr_vessel_hk::CHAR(40) as mr_vessel_hk
    FROM {{ source('dv', 'link_configuration__mr_vessel__mr_vessel_configuration') }}
)

SELECT * FROM source_data 