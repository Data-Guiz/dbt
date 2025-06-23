{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
    SELECT
        personne_hk::CHAR(40) AS personne_hk
        ,load_dts::TIMESTAMP AS load_dts
        ,load_end_dts::TIMESTAMP AS load_end_dts
        ,del_flag::BOOLEAN AS del_flag
        ,hash_diff::VARCHAR(40) AS hash_diff
        ,record_source::VARCHAR(40) AS record_source
        ,dv_run_id::INTEGER AS dv_run_id
        ,id_personne::INTEGER AS id_personne
        ,nom::VARCHAR(64) AS nom
        ,prenom::VARCHAR(64) AS prenom
        ,age::INTEGER AS age
    FROM {{ source('dv', 'hsat_personne') }}
)
SELECT * FROM source_data