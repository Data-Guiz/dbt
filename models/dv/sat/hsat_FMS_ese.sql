{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
    SELECT
        fms_ese_hk::VARCHAR(40) AS fms_ese_hk
        ,load_dts::TIMESTAMP AS load_dts
        ,load_end_dts::TIMESTAMP AS load_end_dts
        ,del_flag::BOOLEAN AS del_flag
        ,hash_diff::VARCHAR(40) AS hash_diff
        ,record_source::VARCHAR(40) AS record_source
        ,dv_run_id::INTEGER AS dv_run_id
        ,ese_no::VARCHAR(20) AS ese_no
        ,ese_name::VARCHAR(100) AS ese_name
        ,ese_date::DATE AS ese_date
        ,ese_port::VARCHAR(40) AS ese_port
        ,ese_duration::VARCHAR(8) AS ese_duration
        ,ese_transdate::DATE AS ese_transdate
    FROM {{ source('dv', 'hsat_fms_ese') }}
)
SELECT * FROM source_data