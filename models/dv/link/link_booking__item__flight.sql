{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['link']
) }}

WITH source_data AS (
    SELECT
    booking__item__flight_hk::VARCHAR(40) AS booking__item__flight_hk
    ,load_dts::TIMESTAMP AS load_dts
    ,record_src::VARCHAR(50) AS record_src
    ,dv_runid::INTEGER AS dv_runid
    ,booking_hk::VARCHAR(40) AS booking_hk
    ,item_hk::VARCHAR(40) AS item_hk
    ,flight_hk::VARCHAR(40) AS flight_hk
    FROM {{ source('dv', 'link_booking__item__flight') }}
)
SELECT * FROM source_data