{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
        booking__item__cruise__vessel__cabin_hk,
        load_dts,
        record_src,
        dv_runid,
        booking_hk,
        item_hk,
        cruise_hk,
        vessel_hk,
        cabin_hk
    FROM {{ source('dv', 'link_booking__item__cruise__vessel__cabin') }}
)

SELECT * FROM source_data 