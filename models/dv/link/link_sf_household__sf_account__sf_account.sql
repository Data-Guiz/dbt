{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['link']
) }}

WITH source_data AS (
    SELECT
    sf_household__sf_account__sf_account_hk::VARCHAR(40) AS sf_household__sf_account__sf_account_hk
    ,load_dts::TIMESTAMP AS load_dts
    ,record_src::VARCHAR(50) AS record_src
    ,dv_runid::INTEGER AS dv_runid
    ,sf_household_hk::VARCHAR(40) AS sf_household_hk
    ,sf_account_customer_hk::VARCHAR(40) AS sf_account_customer_hk
    ,sf_account_headofhousehold_hk::VARCHAR(40) AS sf_account_headofhousehold_hk
    FROM {{ source('dv', 'link_sf_household__sf_account__sf_account') }}
)
SELECT * FROM source_data