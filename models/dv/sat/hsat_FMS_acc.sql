{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
    SELECT
        fms_acc_hk::VARCHAR(40) AS fms_acc_hk
        ,load_dts::TIMESTAMP AS load_dts
        ,load_end_dts::TIMESTAMP AS load_end_dts
        ,del_flag::BOOLEAN AS del_flag
        ,hash_diff::VARCHAR(40) AS hash_diff
        ,record_source::VARCHAR(40) AS record_source
        ,dv_run_id::INTEGER AS dv_run_id
        ,acc_shp_id::INTEGER AS acc_shp_id
        ,acc_cons_id::VARCHAR(50) AS acc_cons_id
        ,acc_tag::VARCHAR(1) AS acc_tag
        ,acc_cab_no::VARCHAR(20) AS acc_cab_no
        ,acc_fstn::VARCHAR(60) AS acc_fstn
        ,acc_name::VARCHAR(40) AS acc_name
        ,acc_transdate::TIMESTAMP AS acc_transdate
        ,acc_cruise::INTEGER AS acc_cruise
        ,acc_loyalty::VARCHAR(24) AS acc_loyalty
    FROM {{ source('dv', 'hsat_fms_acc') }}
)
SELECT * FROM source_data
