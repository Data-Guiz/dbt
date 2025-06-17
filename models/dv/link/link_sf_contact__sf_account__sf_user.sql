{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['link']
) }}

WITH source_data AS (
    SELECT
    sf_contact__sf_account__sf_user_hk::VARCHAR(40) AS sf_contact__sf_account__sf_user_hk
    ,load_dts::TIMESTAMP AS load_dts
    ,record_src::VARCHAR(50) AS record_src
    ,dv_runid::INTEGER AS dv_runid
    ,sf_contact_hk::VARCHAR(40) AS sf_contact_hk
    ,sf_account_hk::VARCHAR(40) AS sf_account_hk
    ,sf_user_hk::VARCHAR(40) AS sf_user_hk
    FROM {{ source('dv', 'link_sf_contact__sf_account__sf_user') }}
)
SELECT * FROM source_data