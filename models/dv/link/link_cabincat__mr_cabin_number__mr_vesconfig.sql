{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
        cabincat__mr_cabin_number__mr_vessel_configuration_hk::CHAR(40) AS cabincat__mr_cabin_number__mr_vessel_configuration_hk,
        load_dts::TIMESTAMP AS load_dts,
        record_src::VARCHAR(50) AS record_src,
        dv_runid::INTEGER AS dv_runid,
        mr_cabin_number_hk::CHAR(40) AS mr_cabin_number_hk,
        mr_vessel_configuration_hk::CHAR(40) AS mr_vessel_configuration_hk,
        cabin_category_hk::CHAR(40) AS cabin_category_hk
    FROM {{ source('dv', 'link_cabincat__mr_cabin_number__mr_vesconfig') }}
)

SELECT * FROM source_data 