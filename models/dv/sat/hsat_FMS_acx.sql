{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
    SELECT
        fms_acx_hk::VARCHAR(40) AS fms_acx_hk
        ,load_dts::TIMESTAMP AS load_dts
        ,load_end_dts::TIMESTAMP AS load_end_dts
        ,del_flag::BOOLEAN AS del_flag
        ,hash_diff::VARCHAR(40) AS hash_diff
        ,record_source::VARCHAR(40) AS record_source
        ,dv_run_id::INTEGER AS dv_run_id
        ,acx_shp_id::INTEGER AS acx_shp_id
        ,acx_transdate::TIMESTAMP AS acx_transdate
        ,acx_a_seaware_client_id::INTEGER AS acx_a_seaware_client_id
        ,acx_nation::VARCHAR(100) AS acx_nation
    FROM {{ source('dv', 'hsat_fms_acx') }}
)
SELECT * FROM source_data
