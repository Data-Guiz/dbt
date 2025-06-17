{{
    config(
        materialized='table',
        schema='dv',
        dist='currency_hk',
        sort=['currency_hk', 'currency_bk']
    )
}}

WITH source_data AS (
    SELECT
        currency_hk::CHAR(40) as currency_hk
        ,load_dts::TIMESTAMP as load_dts
        ,record_source::VARCHAR(40) as record_source
        ,dv_run_id::INTEGER as dv_run_id
        ,currency_bk::VARCHAR(40) as currency_bk
    FROM {{ source('dv', 'hub_currency') }}
)

SELECT 
    * 
FROM source_data