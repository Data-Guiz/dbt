{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
    SELECT
        fms_dep_hk::VARCHAR(40) AS fms_dep_hk
        ,load_dts::TIMESTAMP AS load_dts
        ,load_end_dts::TIMESTAMP AS load_end_dts
        ,del_flag::BOOLEAN AS del_flag
        ,hash_diff::VARCHAR(40) AS hash_diff
        ,record_source::VARCHAR(40) AS record_source
        ,dv_run_id::INTEGER AS dv_run_id
        ,dep_shp_id::INTEGER AS dep_shp_id
        ,dep_no::VARCHAR(10) AS dep_no
        ,dep_mainid::BIGINT AS dep_mainid
        ,dep_flag::VARCHAR(1) AS dep_flag
        ,dep_tag::VARCHAR(2) AS dep_tag
        ,dep_limit::INTEGER AS dep_limit
        ,dep_cur::VARCHAR(10) AS dep_cur
        ,dep_cc_short::VARCHAR(2) AS dep_cc_short
        ,dep_nonref::INTEGER AS dep_nonref
        ,dep_gl_origin::VARCHAR(8) AS dep_gl_origin
        ,dep_gl_department::VARCHAR(8) AS dep_gl_department
        ,dep_gl_controlacc::VARCHAR(8) AS dep_gl_controlacc
        ,dep_gl_subacc::VARCHAR(8) AS dep_gl_subacc
        ,dep_comment::VARCHAR(50) AS dep_comment
        ,dep_transdate::TIMESTAMP AS dep_transdate
        ,dep_dem_id::VARCHAR(10) AS dep_dem_id
        ,dep_pos_code::VARCHAR(10) AS dep_pos_code
        ,dep_pos_id::VARCHAR(20) AS dep_pos_id
        ,dep_pos_period::VARCHAR(10) AS dep_pos_period
        ,dep_pos_type::VARCHAR(3) AS dep_pos_type
    FROM {{ source('dv', 'hsat_fms_dep') }}
)
SELECT * FROM source_data