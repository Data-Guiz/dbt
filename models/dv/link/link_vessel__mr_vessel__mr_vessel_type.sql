{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
        vessel__mr_vessel__mr_vessel_type_hk::CHAR(40) as vessel__mr_vessel__mr_vessel_type_hk,
        load_dts::TIMESTAMP as load_dts,
        record_src::VARCHAR(50) as record_src,
        dv_runid::INTEGER as dv_runid,
        vessel_hk::CHAR(40) as vessel_hk,
        mr_vessel_hk::CHAR(40) as mr_vessel_hk,
        mr_vessel_type_hk::CHAR(40) as mr_vessel_type_hk
    FROM {{ source('dv', 'link_vessel__mr_vessel__mr_vessel_type') }}
)

SELECT * FROM source_data 