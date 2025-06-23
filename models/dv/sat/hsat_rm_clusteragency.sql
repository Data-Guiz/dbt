{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
	SELECT
		clusteragency_hk::CHAR(40) AS clusteragency_hk
		,load_dts::TIMESTAMP AS load_dts
		,load_end_dts::TIMESTAMP AS load_end_dts
		,del_flag::BOOLEAN AS del_flag
		,hash_diff::VARCHAR(40) AS hash_diff
		,record_source::VARCHAR(40) AS record_source
		,dv_run_id::INTEGER AS dv_run_id
		,id::INTEGER AS id
		,editor::VARCHAR(255) AS editor
		,creation_timestamp::TIMESTAMP AS creation_timestamp
		,edition_timestamp::TIMESTAMP AS edition_timestamp
		,agency_code::VARCHAR(255) AS agency_code
		,cluster_agency::VARCHAR(255) AS cluster_agency
		,agency_id::VARCHAR(255) AS agency_id
	FROM {{ source('dv', 'hsat_rm_clusteragency') }}
)
SELECT * FROM source_data