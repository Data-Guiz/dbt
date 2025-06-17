{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['link']
) }}

WITH source_data AS (
    SELECT
    sf_deleted_campaign_members__sf_campaignmember_hk::VARCHAR(40) AS sf_deleted_campaign_members__sf_campaignmember_hk
    ,load_dts::TIMESTAMP AS load_dts
    ,record_src::VARCHAR(50) AS record_src
    ,dv_runid::INTEGER AS dv_runid
    ,sf_deleted_campaign_members_hk::VARCHAR(40) AS sf_deleted_campaign_members_hk
    ,sf_campaignmember_hk::VARCHAR(40) AS sf_campaignmember_hk
    FROM {{ source('dv', 'link_sf_deleted_campaign_members__sf_campaignmember') }}
)
SELECT * FROM source_data