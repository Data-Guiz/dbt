{{
    config(
        materialized='table',
        schema='dv',
        dist='KEY',
        dist_key='vessel_hk',
        sort=['vessel_hk', 'vessel_bk']
    )
}}

WITH source_data AS (
    SELECT
        vessel_hk::CHAR(40) AS vessel_hk,
        load_dts::TIMESTAMP AS load_dts,
        record_source::VARCHAR(40) AS record_source,
        dv_run_id::INTEGER AS dv_run_id,
        vessel_bk::VARCHAR(40) AS vessel_bk
    FROM {{ source('dv', 'hub_vessel') }}
)

SELECT * FROM source_data