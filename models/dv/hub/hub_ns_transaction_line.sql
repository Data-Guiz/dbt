{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
        ns_transaction_line_hk::CHAR(40) as ns_transaction_line_hk
        ,load_dts::TIMESTAMP as load_dts
        ,record_source::VARCHAR(40) as record_source
        ,dv_run_id::INTEGER as dv_run_id
        ,ns_transaction_line_bk::VARCHAR(40) as ns_transaction_line_bk
    FROM {{ source('dv', 'hub_ns_transaction_line') }}
)
SELECT 
    * 
FROM source_data
