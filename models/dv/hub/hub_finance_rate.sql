{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
        finance_rate_hk::CHAR(40) as finance_rate_hk
        ,load_dts::TIMESTAMP as load_dts
        ,record_source::VARCHAR(40) as record_source
        ,dv_run_id::INTEGER as dv_run_id
        ,finance_rate_bk::VARCHAR(40) as finance_rate_bk
    FROM {{ source('dv', 'hub_finance_rate') }}
)

SELECT 
    * 
FROM source_data