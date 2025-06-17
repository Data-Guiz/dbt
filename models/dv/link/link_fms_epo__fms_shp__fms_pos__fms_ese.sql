{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['link']
) }}

WITH source_data AS (
    SELECT
    fms_epo__fms_shp__fms_pos__fms_ese_hk::VARCHAR(40) AS fms_epo__fms_shp__fms_pos__fms_ese_hk
    ,load_dts::TIMESTAMP AS load_dts
    ,record_src::VARCHAR(50) AS record_src
    ,dv_runid::INTEGER AS dv_runid
    ,fms_epo_hk::VARCHAR(40) AS fms_epo_hk
    ,fms_shp_hk::VARCHAR(40) AS fms_shp_hk
    ,fms_pos_hk::VARCHAR(40) AS fms_pos_hk
    ,fms_ese_hk::VARCHAR(40) AS fms_ese_hk
    FROM {{ source('dv', 'link_fms_epo__fms_shp__fms_pos__fms_ese') }}
)
SELECT * FROM source_data
