{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['link']
) }}

WITH source_data AS (
    SELECT
    cruise__theme_hk::VARCHAR(40) AS cruise__theme_hk
    ,load_dts::TIMESTAMP AS load_dts
    ,record_src::VARCHAR(50) AS record_src
    ,dv_runid::INTEGER AS dv_runid
    ,cruise_hk::VARCHAR(40) AS cruise_hk
    ,file_name::VARCHAR(100) AS file_name
    ,datetime_from_file_name::TIMESTAMP AS datetime_from_file_name
    ,cruiseid::VARCHAR(100) AS cruiseid
    ,theme::VARCHAR(100) AS theme
    FROM {{ source('dv', 'nhl_cp_product_theme') }}
)
SELECT * FROM source_data