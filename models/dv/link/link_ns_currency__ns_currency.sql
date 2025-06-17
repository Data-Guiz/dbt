{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['link']
) }}

WITH source_data AS (
    SELECT
    ns_currency__ns_currency_hk::VARCHAR(40) AS ns_currency__ns_currency_hk
    ,load_dts::TIMESTAMP AS load_dts
    ,record_src::VARCHAR(50) AS record_src
    ,dv_runid::INTEGER AS dv_runid
    ,ns_currency_base_hk::VARCHAR(40) AS ns_currency_base_hk
    ,ns_currency_from_hk::VARCHAR(40) AS ns_currency_from_hk
    FROM {{ source('dv', 'link_ns_currency__ns_currency') }}
)
SELECT * FROM source_data