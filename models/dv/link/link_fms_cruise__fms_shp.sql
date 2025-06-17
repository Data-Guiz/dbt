{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['link']
) }}
WITH source_data AS (
    SELECT
    fms_cruise__fms_shp_hk::VARCHAR(40) AS fms_cruise__fms_shp_hk
    ,load_dts::TIMESTAMP AS load_dts
    ,record_src::VARCHAR(50) AS record_src
    ,dv_runid::INTEGER AS dv_runid
    ,fms_cruise_hk::VARCHAR(40) AS fms_cruise_hk
    ,fms_shp_hk::VARCHAR(40) AS fms_shp_hk
    FROM {{ source('dv', 'link_fms_cruise__fms_shp') }}
)
SELECT * FROM source_data