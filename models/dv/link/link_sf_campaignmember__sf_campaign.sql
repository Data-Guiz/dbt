{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['link']
) }}

WITH source_data AS (
    SELECT
    sf_campaignmember__sf_campaign_hk::VARCHAR(40) AS sf_campaignmember__sf_campaign_hk
    ,load_dts::TIMESTAMP AS load_dts
    ,record_src::VARCHAR(50) AS record_src
    ,dv_runid::INTEGER AS dv_runid
    ,sf_campaignmember_hk::VARCHAR(40) AS sf_campaignmember_hk
    ,sf_campaign_hk::VARCHAR(40) AS sf_campaign_hk
    FROM {{ source('dv', 'link_sf_campaignmember__sf_campaign') }}
)
SELECT * FROM source_data