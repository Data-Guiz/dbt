{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
    SELECT
        fms_spa_hk::VARCHAR(40) AS fms_spa_hk
        ,load_dts::TIMESTAMP AS load_dts
        ,load_end_dts::TIMESTAMP AS load_end_dts
        ,del_flag::BOOLEAN AS del_flag
        ,hash_diff::VARCHAR(40) AS hash_diff
        ,record_source::VARCHAR(40) AS record_source
        ,dv_run_id::INTEGER AS dv_run_id
        ,spa_transdate::DATE AS spa_transdate
        ,spa_cabin::VARCHAR(12) AS spa_cabin
        ,spa_description::VARCHAR(100) AS spa_description
        ,spa_duration::INTEGER AS spa_duration
        ,spa_status::VARCHAR(1) AS spa_status
        ,spa_time::INTEGER AS spa_time
        ,spa_date::DATE AS spa_date
        ,spa_booking_date::DATE AS spa_booking_date
        ,spa_booking_origin::VARCHAR(10) AS spa_booking_origin
        ,spa_item_type::VARCHAR(1) AS spa_item_type
        ,spa_prepaid::VARCHAR(1) AS spa_prepaid
    FROM {{ source('dv', 'hsat_fms_spa') }}
)
SELECT * FROM source_data