{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
	SELECT
		item_hk::CHAR(40) as item_hk
		,load_dts::TIMESTAMP as load_dts
		,record_source::VARCHAR(40) as record_source
		,dv_run_id::INTEGER as dv_run_id
		,item_bk::VARCHAR(40) as item_bk
	FROM {{ source('dv', 'hub_item_old') }}
)

SELECT 
    * 
FROM source_data