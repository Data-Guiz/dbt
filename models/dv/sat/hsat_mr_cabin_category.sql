 {{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
    SELECT
		mr_cabin_category_hk::VARCHAR(40) AS mr_cabin_category_hk
		,load_dts::TIMESTAMP AS load_dts
		,load_end_dts::TIMESTAMP AS load_end_dts
		,del_flag::BOOLEAN AS del_flag
		,hash_diff::VARCHAR(40) AS hash_diff
		,record_source::VARCHAR(40) AS record_source
		,dv_run_id::INTEGER AS dv_run_id
		,creation_timestamp::TIMESTAMP AS creation_timestamp
		,edition_timestamp::TIMESTAMP AS edition_timestamp
		,editor::VARCHAR(30) AS editor
		,cabin_code_pricing::VARCHAR(10) AS cabin_code_pricing
		,cabin_code_ibs_equiv::VARCHAR(10) AS cabin_code_ibs_equiv
		,room_name::VARCHAR(50) AS room_name
		,chart_order_id::INTEGER AS chart_order_id
		,cabin_type::VARCHAR(20) AS cabin_type
		,capacity_pax::INTEGER AS capacity_pax
		,cabin_total_cabin_code_ibs_equiv::INTEGER AS cabin_total_cabin_code_ibs_equiv
    FROM {{ source('dv', 'hsat_mr_cabin_category') }}
)
SELECT * FROM source_data
