{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['link']
) }}
WITH source_data AS (

    SELECT
    booking__contact_hk::VARCHAR(40) AS booking__contact_hk
    ,load_dts::TIMESTAMP AS load_dts
    ,record_src::VARCHAR(50) AS record_src
    ,dv_runid::INTEGER AS dv_runid
    ,booking_hk::VARCHAR(40) AS booking_hk
    ,contact_hk::VARCHAR(40) AS contact_hk
    FROM {{ source('dv', 'link_booking__contact') }}
)
SELECT * FROM source_data