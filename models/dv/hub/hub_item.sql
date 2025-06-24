{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
        item_hk::CHAR(40) AS item_hk,
        load_dts::TIMESTAMP AS load_dts,
        record_source::VARCHAR(40) AS record_source,
        dv_run_id::INTEGER AS dv_run_id,
        item_bk::VARCHAR(40) AS item_bk
    FROM {{ source('dv', 'hub_item') }}
)

SELECT * FROM source_data