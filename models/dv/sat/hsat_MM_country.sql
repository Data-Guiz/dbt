{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
    SELECT
        country_hk::VARCHAR(40) AS country_hk
        ,load_dts::TIMESTAMP AS load_dts
        ,load_end_dts::TIMESTAMP AS load_end_dts
        ,del_flag::BOOLEAN AS del_flag
        ,hash_diff::VARCHAR(40) AS hash_diff
        ,record_source::VARCHAR(40) AS record_source
        ,dv_run_id::INTEGER AS dv_run_id
        ,id::INTEGER AS id
        ,creation_timestamp::TIMESTAMP AS creation_timestamp
        ,edition_timestamp::TIMESTAMP AS edition_timestamp
        ,editor::VARCHAR(255) AS editor
        ,uid::VARCHAR(255) AS uid
        ,source_pax_1::VARCHAR(255) AS source_pax_1
        ,source_pax_group::VARCHAR(255) AS source_pax_group
        ,source_pax_zone::VARCHAR(255) AS source_pax_zone
        ,source_pax_2::VARCHAR(255) AS source_pax_2
        ,source_pax_3::VARCHAR(255) AS source_pax_3
    FROM {{ source('dv', 'hsat_mm_country') }}
)
SELECT * FROM source_data