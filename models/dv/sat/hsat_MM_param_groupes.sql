{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
    SELECT
    	group_hk::VARCHAR(40) AS group_hk
    	,load_dts::TIMESTAMP AS load_dts
    	,load_end_dts::TIMESTAMP AS load_end_dts
    	,del_flag::BOOLEAN AS del_flag
    	,hash_diff::VARCHAR(40) AS hash_diff
    	,record_source::VARCHAR(40) AS record_source
    	,dv_run_id::INTEGER AS dv_run_id
    	,id::INTEGER AS id
    	,uid::VARCHAR(255) AS uid
    	,editor::VARCHAR(255) AS editor
    	,creation_timestamp::TIMESTAMP AS creation_timestamp
    	,edition_timestamp::TIMESTAMP AS edition_timestamp
    	,gp_group_fin::VARCHAR(255) AS gp_group_fin
    	,gp_group_large::VARCHAR(255) AS gp_group_large
    	,oot::VARCHAR(255) AS oot
    FROM {{ source('dv', 'hsat_mm_param_groupes') }}
)
SELECT * FROM source_data