{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
	    brand_hk::VARCHAR(40) AS brand_hk
	    ,load_dts::TIMESTAMP AS load_dts
	    ,record_source::VARCHAR(40) AS record_source
	    ,dv_run_id::INTEGER AS dv_run_id
	    ,brand_bk::VARCHAR(40) AS brand_bk
    FROM {{ source('dv', 'hub_brand') }}
)
SELECT 
    * 
FROM source_data 