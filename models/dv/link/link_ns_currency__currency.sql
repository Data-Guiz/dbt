{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['link']
) }}

WITH source_data AS (
    SELECT
    ns_currency__currency_hk::VARCHAR(40) AS ns_currency__currency_hk
    ,load_dts::TIMESTAMP AS load_dts
    ,record_src::VARCHAR(50) AS record_src
    ,dv_runid::INTEGER AS dv_runid
    ,ns_currency_hk::VARCHAR(40) AS ns_currency_hk
    ,currency_hk::VARCHAR(40) AS currency_hk
    FROM {{ source('dv', 'link_ns_currency__currency') }}
)
SELECT * FROM source_data