{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
        cruise_hk::CHAR(40) AS cruise_hk,
        load_dts::TIMESTAMP AS load_dts,
        record_source::VARCHAR(40) AS record_source,
        dv_run_id::INTEGER AS dv_run_id,
        cruise_bk::VARCHAR(40) AS cruise_bk
    FROM {{ source('dv', 'hub_cruise') }}
)

SELECT * FROM source_data