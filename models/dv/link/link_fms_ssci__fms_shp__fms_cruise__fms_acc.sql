{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['link']
) }}

WITH source_data AS (
    SELECT
    fms_ssci__fms_shp__fms_cruise__fms_acc_hk::VARCHAR(40) AS fms_ssci__fms_shp__fms_cruise__fms_acc_hk
    ,load_dts::TIMESTAMP AS load_dts
    ,record_src::VARCHAR(50) AS record_src
    ,dv_runid::INTEGER AS dv_runid
    ,fms_ssci_hk::VARCHAR(40) AS fms_ssci_hk
    ,fms_shp_hk::VARCHAR(40) AS fms_shp_hk
    ,fms_cruise_hk::VARCHAR(40) AS fms_cruise_hk
    ,fms_acc_hk::VARCHAR(40) AS fms_acc_hk
    FROM {{ source('dv', 'link_fms_ssci__fms_shp__fms_cruise__fms_acc') }}
)
SELECT * FROM source_data