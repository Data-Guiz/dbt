{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['link']
) }}

WITH source_data AS (
    SELECT
    group__cabin_category_hk::VARCHAR(40) AS group__cabin_category_hk
    ,load_dts::TIMESTAMP AS load_dts
    ,record_src::VARCHAR(50) AS record_src
    ,dv_runid::INTEGER AS dv_runid
    ,group_hk::VARCHAR(40) AS group_hk
    ,cabin_category_hk::VARCHAR(40) AS cabin_category_hk
    FROM {{ source('dv', 'link_group__cabin_category') }}
)
SELECT * FROM source_data