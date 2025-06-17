{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['link']
) }}

WITH source_data AS (
    SELECT
    ns_transaction__ns_transaction_line_hk::VARCHAR(40) AS ns_transaction__ns_transaction_line_hk
    ,load_dts::TIMESTAMP AS load_dts
    ,record_src::VARCHAR(50) AS record_src
    ,dv_runid::INTEGER AS dv_runid
    ,ns_transaction_hk::VARCHAR(40) AS ns_transaction_hk
    ,ns_transaction_line_hk::VARCHAR(40) AS ns_transaction_line_hk
    FROM {{ source('dv', 'link_ns_transaction__ns_transaction_line') }}
)
SELECT * FROM source_data
