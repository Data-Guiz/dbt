{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
    SELECT
        mr_vessel_type_hk::CHAR(40) AS mr_vessel_type_hk
        ,load_dts::TIMESTAMP AS load_dts
        ,load_end_dts::TIMESTAMP AS load_end_dts
        ,del_flag::BOOLEAN AS del_flag
        ,hash_diff::VARCHAR(40) AS hash_diff
        ,record_source::VARCHAR(40) AS record_source
        ,dv_run_id::INTEGER AS dv_run_id
        ,creation_timestamp::TIMESTAMP AS creation_timestamp
        ,edition_timestamp::TIMESTAMP AS edition_timestamp
        ,editor::VARCHAR(30) AS editor
        ,vessel_type::VARCHAR(30) AS vessel_type
		,competitor::VARCHAR(5) AS competitor
    FROM {{ source('dv', 'hsat_mr_vessel_type') }}
)
SELECT * FROM source_data
