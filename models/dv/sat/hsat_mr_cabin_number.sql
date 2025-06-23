{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
        mr_cabin_number_hk::CHAR(40) AS mr_cabin_number_hk
        ,load_dts::TIMESTAMP AS load_dts
        ,load_end_dts::TIMESTAMP AS load_end_dts
        ,del_flag::BOOLEAN AS del_flag
        ,hash_diff::VARCHAR(40) AS hash_diff
        ,record_source::VARCHAR(40) AS record_source
        ,dv_run_id::INTEGER AS dv_run_id
        ,creation_timestamp::TIMESTAMP AS creation_timestamp
        ,edition_timestamp::TIMESTAMP AS edition_timestamp
        ,editor::VARCHAR(30) AS editor
        ,cabin_no::VARCHAR(20) AS cabin_no
        ,cabin_id::INTEGER AS cabin_id
        ,cabin_shipboard::VARCHAR(10) AS cabin_shipboard
        ,cabin_position::VARCHAR(10) AS cabin_position
        ,deck_code::VARCHAR(10) AS deck_code
        ,capacity_cabin::INTEGER AS capacity_cabin
        ,capacity_pax::INTEGER AS capacity_pax
        ,nb_pax_adult::INTEGER AS nb_pax_adult
        ,nb_pax_additional::INTEGER AS nb_pax_additional
    FROM {{ source('dv', 'hsat_mr_cabin_number') }}
)

SELECT * FROM source_data 