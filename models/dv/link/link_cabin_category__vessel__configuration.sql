{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['link']
) }}

WITH source_data AS (
    SELECT
    cabin_category__vessel__configuration_hk::VARCHAR(40) AS cabin_category__vessel__configuration_hk
    ,load_dts::TIMESTAMP AS load_dts
    ,record_src::VARCHAR(50) AS record_src
    ,dv_runid::INTEGER AS dv_runid
    ,cabin_category_hk::VARCHAR(40) AS cabin_category_hk
    ,vessel_hk::VARCHAR(40) AS vessel_hk
    ,configuration_hk::VARCHAR(40) AS configuration_hk
    FROM {{ source('dv', 'link_cabin_category__vessel__configuration') }}
)
SELECT * FROM source_data