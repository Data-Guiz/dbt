{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
        booking_hk::CHAR(40) AS booking_hk,
        load_dts::TIMESTAMP AS load_dts,
        record_source::VARCHAR(40) AS record_source,
        dv_run_id::INTEGER AS dv_run_id,
        booking_bk::VARCHAR(40) AS booking_bk
    FROM {{ source('dv', 'hub_booking') }}
)
SELECT
    * 
FROM source_data 