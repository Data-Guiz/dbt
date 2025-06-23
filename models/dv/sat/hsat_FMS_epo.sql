{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
    SELECT
        fms_epo_hk::VARCHAR(40) AS fms_epo_hk
        ,load_dts::TIMESTAMP AS load_dts
        ,load_end_dts::TIMESTAMP AS load_end_dts
        ,del_flag::BOOLEAN AS del_flag
        ,hash_diff::VARCHAR(40) AS hash_diff
        ,record_source::VARCHAR(40) AS record_source
        ,dv_run_id::INTEGER AS dv_run_id
        ,epo_pos_id::BIGINT AS epo_pos_id
        ,epo_quant::INTEGER AS epo_quant
        ,epo_prepaid::VARCHAR(1) AS epo_prepaid
        ,epo_void::INTEGER AS epo_void
        ,epo_transdate::DATE AS epo_transdate
    FROM {{ source('dv', 'hsat_fms_epo') }}
)
SELECT * FROM source_data