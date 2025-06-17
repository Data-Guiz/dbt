{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['link']
) }}
WITH source_data AS (
    SELECT
    cruise_hk__cruise_cluster_hk::VARCHAR(40) AS cruise_hk__cruise_cluster_hk
    ,load_dts::TIMESTAMP AS load_dts
    ,record_src::VARCHAR(50) AS record_src
    ,dv_runid::INTEGER AS dv_runid
    ,cruise_hk::VARCHAR(40) AS cruise_hk
    ,cruise_cluster_hk::VARCHAR(40) AS cruise_cluster_hk
    FROM {{ source('dv', 'link_cruise__cruise_cluster') }}
)
SELECT * FROM source_data