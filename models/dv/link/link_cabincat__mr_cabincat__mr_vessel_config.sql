{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['link']
) }}

WITH source_data AS (
    SELECT
    cabincat__mr_cabincat__mr_vesconfig_hk::VARCHAR(40) AS cabincat__mr_cabincat__mr_vesconfig_hk
    ,load_dts::TIMESTAMP AS load_dts
    ,record_src::VARCHAR(50) AS record_src
    ,dv_runid::INTEGER AS dv_runid
    ,cabin_category_hk::VARCHAR(40) AS cabin_category_hk
	,mr_cabin_category_hk::VARCHAR(40) AS mr_cabin_category_hk
    ,mr_vessel_configuration_hk::VARCHAR(40) AS mr_vessel_configuration_hk
    FROM {{ source('dv', 'link_cabincat__mr_cabincat__mr_vessel_config') }}
)
SELECT * FROM source_data