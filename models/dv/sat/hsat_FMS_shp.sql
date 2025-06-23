{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
    SELECT
        fms_shp_hk::VARCHAR(40) AS fms_shp_hk
        ,load_dts::TIMESTAMP AS load_dts
        ,load_end_dts::TIMESTAMP AS load_end_dts
        ,del_flag::BOOLEAN AS del_flag
        ,hash_diff::VARCHAR(40) AS hash_diff
        ,record_source::VARCHAR(40) AS record_source
        ,dv_run_id::INTEGER AS dv_run_id
        ,shp_company::VARCHAR(50) AS shp_company
        ,shp_name::VARCHAR(50) AS shp_name
        ,shp_paxno_pday::INTEGER AS shp_paxno_pday
        ,shp_crwno_pday::INTEGER AS shp_crwno_pday
        ,shp_recv_queue::VARCHAR(200) AS shp_recv_queue
        ,shp_sync_date::TIMESTAMP AS shp_sync_date
        ,shp_class_id::VARCHAR(10) AS shp_class_id
        ,shp_categ_id::VARCHAR(10) AS shp_categ_id
        ,shp_short_id::VARCHAR(4) AS shp_short_id
        ,shp_short_id1::VARCHAR(4) AS shp_short_id1
        ,shp_transdate::TIMESTAMP AS shp_transdate
        ,shp_show_emergency::VARCHAR(1) AS shp_show_emergency
        ,shp_emergency_date::DATE AS shp_emergency_date
        ,shp_security_class::VARCHAR(50) AS shp_security_class
        ,shp_active::VARCHAR(1) AS shp_active
    FROM {{ source('dv', 'hsat_fms_shp') }}
)
SELECT * FROM source_data