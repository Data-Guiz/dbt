{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['link']
) }}

WITH source_data AS (
    SELECT
    mm_cruise__cruise__vessel_hk::VARCHAR(40) AS mm_cruise__cruise__vessel_hk
    ,load_dts::TIMESTAMP AS load_dts
    ,record_src::VARCHAR(50) AS record_src
    ,dv_runid::INTEGER AS dv_runid
    ,mm_cruise_hk::VARCHAR(40) AS mm_cruise_hk
    ,cruise_hk::VARCHAR(40) AS cruise_hk
    ,vessel_hk::VARCHAR(40) AS vessel_hk
    FROM {{ source('dv', 'link_mm_cruise__cruise__vessel') }}
)
SELECT * FROM source_data