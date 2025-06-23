{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
    SELECT
        ns_transaction_hk::CHAR(40) AS ns_transaction_hk
        ,load_dts::TIMESTAMP AS load_dts
        ,load_end_dts::TIMESTAMP AS load_end_dts
        ,del_flag::BOOLEAN AS del_flag
        ,hash_diff::VARCHAR(40) AS hash_diff
        ,record_source::VARCHAR(40) AS record_source
        ,dv_run_id::INTEGER AS dv_run_id
        ,deleted_date::VARCHAR(100) AS deleted_date
        ,deleted_by::VARCHAR(100) AS deleted_by
        ,record_type::VARCHAR(100) AS record_type
        ,name::VARCHAR(100) AS name
    FROM {{ source('dv', 'hsat_ns_lmm_deleted') }}
)
SELECT * FROM source_data