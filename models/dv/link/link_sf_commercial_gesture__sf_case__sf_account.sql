{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['link']
) }}

WITH source_data AS (
    SELECT
    sf_commercial_gesture__sf_case__sf_account_hk::VARCHAR(40) AS sf_commercial_gesture__sf_case__sf_account_hk
    ,load_dts::TIMESTAMP AS load_dts
    ,record_src::VARCHAR(50) AS record_src
    ,dv_runid::INTEGER AS dv_runid
    ,sf_commercial_gesture_hk::VARCHAR(40) AS sf_commercial_gesture_hk
    ,sf_case_hk::VARCHAR(40) AS sf_case_hk
    ,sf_account_hk::VARCHAR(40) AS sf_account_hk
    FROM {{ source('dv', 'link_sf_commercial_gesture__sf_case__sf_account') }}
)
SELECT * FROM source_data