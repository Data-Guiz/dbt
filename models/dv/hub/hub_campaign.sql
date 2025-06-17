{{
    config(
        materialized='table',
        schema='dv',
        dist='campaign_hk',
        sort=['campaign_hk', 'campaign_bk']
    )
}}

WITH source_data AS (
    SELECT
        campaign_hk::CHAR(40) as campaign_hk
        ,load_dts::TIMESTAMP as load_dts
        ,record_source::VARCHAR(40) as record_source
        ,dv_run_id::INTEGER as dv_run_id
        ,campaign_bk::VARCHAR(40) as campaign_bk
    FROM {{ source('dv', 'hub_campaign') }}
)

SELECT 
    * 
FROM source_data