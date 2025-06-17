{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
        mm_cruise__cruise__vessel_hk::CHAR(40) AS mm_cruise__cruise__vessel_hk,
        load_dts::TIMESTAMP AS load_dts,
        load_end_dts::TIMESTAMP AS load_end_dts,
        del_flag::BOOLEAN AS del_flag,
        hash_diff::VARCHAR(40) AS hash_diff,
        record_source::VARCHAR(40) AS record_source,
        dv_run_id::INTEGER AS dv_run_id
    FROM {{ source('dv', 'lsat_MM_cruise') }}
)

SELECT * FROM source_data 