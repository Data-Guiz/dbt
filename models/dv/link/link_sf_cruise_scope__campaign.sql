{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['link']
) }}

WITH source_data AS (
    SELECT
    sf_cruise_scope__campaign_hk::VARCHAR(40) AS sf_cruise_scope__campaign_hk
    ,load_dts::TIMESTAMP AS load_dts
    ,record_src::VARCHAR(50) AS record_src
    ,dv_runid::INTEGER AS dv_runid
    ,sf_cruise_scope_hk::VARCHAR(40) AS sf_cruise_scope_hk
    ,sf_campaign_hk::VARCHAR(40) AS sf_campaign_hk
    FROM {{ source('dv', 'link_sf_cruise_scope__campaign') }}
)
SELECT * FROM source_data