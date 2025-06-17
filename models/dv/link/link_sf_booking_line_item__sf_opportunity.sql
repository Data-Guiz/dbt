{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['link']
) }}

WITH source_data AS (
    SELECT
    sf_booking_line_item__sf_opportunity_hk::VARCHAR(40) AS sf_booking_line_item__sf_opportunity_hk
    ,load_dts::TIMESTAMP AS load_dts
    ,record_src::VARCHAR(50) AS record_src
    ,dv_runid::INTEGER AS dv_runid
    ,sf_booking_line_item_hk::VARCHAR(40) AS sf_booking_line_item_hk
    ,sf_opportunity_hk::VARCHAR(40) AS sf_opportunity_hk
    FROM {{ source('dv', 'link_sf_booking_line_item__sf_opportunity') }}
)
SELECT * FROM source_data