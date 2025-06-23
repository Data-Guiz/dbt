{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
    SELECT
        fms_dem_hk::VARCHAR(40) AS fms_dem_hk
        ,load_dts::TIMESTAMP AS load_dts
        ,load_end_dts::TIMESTAMP AS load_end_dts
        ,del_flag::BOOLEAN AS del_flag
        ,hash_diff::VARCHAR(40) AS hash_diff
        ,record_source::VARCHAR(40) AS record_source
        ,dv_run_id::INTEGER AS dv_run_id
        ,dem_shp_id::INTEGER AS dem_shp_id
        ,dem_no::VARCHAR(10) AS dem_no
        ,dem_flag::VARCHAR(1) AS dem_flag
        ,dem_comment::VARCHAR(50) AS dem_comment
        ,dem_transdate::TIMESTAMP AS dem_transdate
    FROM {{ source('dv', 'hsat_fms_dem') }}
)
SELECT * FROM source_data