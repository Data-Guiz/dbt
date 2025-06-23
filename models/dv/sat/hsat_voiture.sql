{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
	SELECT
		voiture_hk::CHAR(40) AS voiture_hk
		,load_dts::TIMESTAMP AS load_dts
		,load_end_dts::TIMESTAMP AS load_end_dts
		,del_flag::BOOLEAN AS del_flag
		,hash_diff::VARCHAR(40) AS hash_diff
		,record_source::VARCHAR(40) AS record_source
		,dv_run_id::INTEGER AS dv_run_id
		,moteur::VARCHAR(64) AS moteur
		,marque::VARCHAR(64) AS marque
		,modele::VARCHAR(64) AS modèle
	FROM {{ source('dv', 'hsat_voiture') }}
)
SELECT * FROM source_data