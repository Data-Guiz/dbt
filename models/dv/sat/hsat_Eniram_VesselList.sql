{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
	SELECT
		eniram_vessel_hk::VARCHAR(40) AS eniram_vessel_hk
		,load_dts::TIMESTAMP AS load_dts
		,load_end_dts::TIMESTAMP AS load_end_dts
		,del_flag::BOOLEAN AS del_flag
		,hash_diff::VARCHAR(40) AS hash_diff
		,record_source::VARCHAR(40) AS record_source
		,dv_run_id::INTEGER AS dv_run_id
		,vessel_name::VARCHAR(40) AS vessel_name
	FROM {{ source('dv', 'hsat_eniram_vessellist') }}
)
SELECT * FROM source_data