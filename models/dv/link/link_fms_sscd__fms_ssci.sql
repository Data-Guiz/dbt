{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['link']
) }}

WITH source_data AS (
    SELECT
    fms_sscd__fms_ssci_hk::VARCHAR(40) AS fms_sscd__fms_ssci_hk
    ,load_dts::TIMESTAMP AS load_dts
    ,record_src::VARCHAR(50) AS record_src
    ,dv_runid::INTEGER AS dv_runid
    ,fms_sscd_hk::VARCHAR(40) AS fms_sscd_hk
    ,fms_ssci_hk::VARCHAR(40) AS fms_ssci_hk
    FROM {{ source('dv', 'link_fms_sscd__fms_ssci') }}
)
SELECT * FROM source_data