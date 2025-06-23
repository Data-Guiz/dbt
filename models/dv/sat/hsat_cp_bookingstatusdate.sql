{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
    SELECT
        booking_hk::VARCHAR(40) AS booking_hk
        ,load_dts::TIMESTAMP AS load_dts
        ,load_end_dts::TIMESTAMP AS load_end_dts
        ,del_flag::BOOLEAN AS del_flag
        ,hash_diff::VARCHAR(40) AS hash_diff
        ,record_source::VARCHAR(40) AS record_source
        ,dv_run_id::INTEGER AS dv_run_id
        ,file_name::VARCHAR(100) AS file_name
        ,datetime_from_file_name::TIMESTAMP AS datetime_from_file_name
        ,cxldate::VARCHAR(100) AS cxldate
        ,cxldate__date::DATE AS cxldate__date
    FROM {{ source('dv', 'hsat_cp_bookingstatusdate') }}
)
SELECT * FROM source_data