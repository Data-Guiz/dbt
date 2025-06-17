{{
    materialized='table',
    schema='dv',
    database='dev',
    tags=['link']
}}

WITH source_data AS (
    SELECT
    fms_pos__fms_cruise__fms_shp__fms_dep__fms_dem_hk::VARCHAR(40) AS fms_pos__fms_cruise__fms_shp__fms_dep__fms_dem_hk
    ,load_dts::TIMESTAMP AS load_dts
    ,record_src::VARCHAR(50) AS record_src
    ,dv_runid::INTEGER AS dv_runid
    ,fms_pos_hk::VARCHAR(40) AS fms_pos_hk
    ,fms_cruise_hk::VARCHAR(40) AS fms_cruise_hk
    ,fms_shp_hk::VARCHAR(40) AS fms_shp_hk
    ,fms_dep_hk::VARCHAR(40) AS fms_dep_hk
    ,fms_dem_hk::VARCHAR(40) AS fms_dem_hk
    FROM {{ source('dv', 'link_fms_pos__fms_cruise__fms_shp__fms_dep__fms_dem') }}
)
SELECT * FROM source_data