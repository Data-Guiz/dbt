{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
        ns_transaction_hk::CHAR(40) as ns_transaction_hk
        ,load_dts::TIMESTAMP as load_dts
        ,record_source::VARCHAR(40) as record_source
        ,dv_run_id::INTEGER as dv_run_id
        ,ns_transaction_bk::VARCHAR(40) as ns_transaction_bk
    FROM {{ source('dv', 'hub_ns_transaction') }}
)
SELECT 
    * 
FROM source_data