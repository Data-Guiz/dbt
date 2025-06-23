{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
	SELECT
		id_erosionhorsibs_hk::CHAR(40) AS id_erosionhorsibs_hk
		,load_dts::TIMESTAMP AS load_dts
		,load_end_dts::TIMESTAMP AS load_end_dts
		,del_flag::BOOLEAN AS del_flag
		,hash_diff::VARCHAR(40) AS hash_diff
		,record_source::VARCHAR(40) AS record_source
		,dv_run_id::INTEGER AS dv_run_id
		,editor::VARCHAR(255) AS editor
		,creation_timestamp::TIMESTAMP AS creation_timestamp
		,edition_timestamp::TIMESTAMP AS edition_timestamp
		,"type"::VARCHAR(255) AS "type"
		,description::VARCHAR(255) AS description
		,"year"::VARCHAR(255) AS "year"
		,client_type::VARCHAR(255) AS client_type
		,"zone"::VARCHAR(255) AS "zone"
		,amount::DOUBLE PRECISION AS amount
		,allocation_key::VARCHAR(255) AS allocation_key
		,cruise_code::VARCHAR(255) AS cruise_code
		,cruise_code__cluster::VARCHAR(255) AS cruise_code__cluster
		,cruise_code__code::VARCHAR(255) AS cruise_code__code
		,cluster_agency::VARCHAR(255) AS cluster_agency
		,cluster_rate::VARCHAR(255) AS cluster_rate
		,cluster_discount::VARCHAR(255) AS cluster_discount
		,cluster_usgroup::VARCHAR(255) AS cluster_usgroup
		,cluster_usgroup__cluster::VARCHAR(255) AS cluster_usgroup__cluster
		,cluster_usgroup__code::VARCHAR(255) AS cluster_usgroup__code
	FROM {{ source('dv', 'hsat_rm_erosionhorsibs') }}
)
SELECT * FROM source_data