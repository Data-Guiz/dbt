{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['link']
) }}

WITH source_data AS (
    SELECT
    currency__finance_rate_hk::VARCHAR(40) AS currency__finance_rate_hk
    ,load_dts::TIMESTAMP AS load_dts
    ,record_src::VARCHAR(50) AS record_src
    ,dv_runid::INTEGER AS dv_runid
    ,currency_hk::VARCHAR(40) AS currency_hk
    ,finance_rate_hk::VARCHAR(40) AS finance_rate_hk
    FROM {{ source('dv', 'link_currency__finance_rate') }}
)
SELECT * FROM source_data
