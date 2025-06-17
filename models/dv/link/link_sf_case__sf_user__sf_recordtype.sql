{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['link']
) }}

WITH source_data AS (
    SELECT
    sf_case__sf_user__sf_recordtype_hk::VARCHAR(40) AS sf_case__sf_user__sf_recordtype_hk
    ,load_dts::TIMESTAMP AS load_dts
    ,record_src::VARCHAR(50) AS record_src
    ,dv_runid::INTEGER AS dv_runid
    ,sf_case_hk::VARCHAR(40) AS sf_case_hk
    ,sf_user_hk::VARCHAR(40) AS sf_user_hk
    ,sf_recordtype_hk::VARCHAR(40) AS sf_recordtype_hk
    FROM {{ source('dv', 'link_sf_case__sf_user__sf_recordtype') }}
)
SELECT * FROM source_data