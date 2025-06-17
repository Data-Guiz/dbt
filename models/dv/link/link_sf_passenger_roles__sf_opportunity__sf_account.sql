{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['link']
) }}

WITH source_data AS (
    SELECT
    sf_passenger_roles__sf_opportunity__sf_account_hk::VARCHAR(40) AS sf_passenger_roles__sf_opportunity__sf_account_hk
    ,load_dts::TIMESTAMP AS load_dts
    ,record_src::VARCHAR(50) AS record_src
    ,dv_runid::INTEGER AS dv_runid
    ,sf_passenger_roles_hk::VARCHAR(40) AS sf_passenger_roles_hk
    ,sf_opportunity_hk::VARCHAR(40) AS sf_opportunity_hk
    ,sf_account_hk::VARCHAR(40) AS sf_account_hk
    FROM {{ source('dv', 'link_sf_passenger_roles__sf_opportunity__sf_account') }}
)
SELECT * FROM source_data