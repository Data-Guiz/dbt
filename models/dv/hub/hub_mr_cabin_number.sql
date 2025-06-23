{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
        mr_cabin_number_hk::CHAR(40) as mr_cabin_number_hk
        ,load_dts::TIMESTAMP as load_dts
        ,record_source::VARCHAR(40) as record_source
        ,dv_run_id::INTEGER as dv_run_id
        ,cabin_number_id::VARCHAR(40) as cabin_number_id
    FROM {{ source('dv', 'hub_mr_cabin_number') }}
)

SELECT 
    * 
FROM source_data







--DROP TABLE dv.hub_mr_cabin_number;

CREATE TABLE IF NOT EXISTS dv.hub_mr_cabin_number
(
	mr_cabin_number_hk CHAR(40) NOT NULL  ENCODE zstd
	,load_dts TIMESTAMP WITHOUT TIME ZONE NOT NULL  ENCODE zstd
	,record_source VARCHAR(40) NOT NULL  ENCODE zstd
	,dv_run_id INTEGER NOT NULL  ENCODE zstd
	,cabin_number_id VARCHAR(40) NOT NULL  ENCODE zstd
	,PRIMARY KEY (mr_cabin_number_hk)
)
DISTSTYLE KEY
 DISTKEY (mr_cabin_number_hk)
 SORTKEY (
	mr_cabin_number_hk,
	cabin_number_id
	)
;
