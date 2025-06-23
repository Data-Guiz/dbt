{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
    SELECT
    	mm_cruise_hk::VARCHAR(40) AS mm_cruise_hk
    	,load_dts::TIMESTAMP AS load_dts
    	,load_end_dts::TIMESTAMP AS load_end_dts
    	,del_flag::BOOLEAN AS del_flag
    	,hash_diff::VARCHAR(40) AS hash_diff
    	,record_source::VARCHAR(40) AS record_source
    	,dv_run_id::INTEGER AS dv_run_id
    	,cruise_modification_date::DATE AS cruise_modification_date
    	,cruise_modification_status::VARCHAR(255) AS cruise_modification_status
    FROM {{ source('dv', 'hsat_mm_cruisemodification') }}
)
SELECT * FROM source_data