{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
        sf_subscription__brand_hk::CHAR(40) as sf_subscription__brand_hk,
        load_dts::TIMESTAMP as load_dts,
        record_src::VARCHAR(50) as record_src,
        dv_runid::INTEGER as dv_runid,
        sf_subscription_hk::CHAR(40) as sf_subscription_hk,
        brand_hk::CHAR(40) as brand_hk
    FROM {{ source('dv', 'link_sf_subscription__brand') }}
)

SELECT * FROM source_data 