{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
    SELECT
		vessel_hk::VARCHAR(40) AS vessel_hk
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
		,vessel_code::VARCHAR(255) AS vessel_code
		,vessel_name::VARCHAR(255) AS vessel_name
		,vessel_long_name::VARCHAR(255) AS vessel_long_name
		,vessel_group::VARCHAR(255) AS vessel_group
		,cabin_capacity::INTEGER AS cabin_capacity
    FROM {{ source('dv', 'hsat_mm_vessel') }}
)
SELECT * FROM source_data