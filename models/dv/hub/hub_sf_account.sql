{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
        sf_account_hk::CHAR(40) AS sf_account_hk
        ,load_dts::TIMESTAMP AS load_dts
        ,record_source::VARCHAR(40) AS record_source
        ,dv_run_id::INTEGER AS dv_run_id
        ,sf_account_bk::VARCHAR(40) AS sf_account_bk
    FROM {{ source('dv', 'hub_sf_account') }}
)
SELECT 
    * 
FROM source_data 