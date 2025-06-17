{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['link']
) }}

WITH source_data AS (
    SELECT
    sf_campaign__campaign__sf_user_hk::VARCHAR(40) AS sf_campaign__campaign__sf_user_hk
    ,load_dts::TIMESTAMP AS load_dts
    ,record_src::VARCHAR(50) AS record_src
    ,dv_runid::INTEGER AS dv_runid
    ,sf_campaign_hk::VARCHAR(40) AS sf_campaign_hk
    ,campaign_hk::VARCHAR(40) AS campaign_hk
    ,sf_user_hk::VARCHAR(40) AS sf_user_hk
    FROM {{ source('dv', 'link_sf_campaign__campaign__sf_user') }}
)
SELECT * FROM source_data