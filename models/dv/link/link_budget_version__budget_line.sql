{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['link']
) }}

WITH source_data AS (
    SELECT
    budget_version__budget_line_hk::VARCHAR(40) AS budget_version__budget_line_hk
    ,load_dts::TIMESTAMP AS load_dts
    ,record_src::VARCHAR(50) AS record_src
    ,dv_runid::INTEGER AS dv_runid
    ,budget_version_hk::VARCHAR(40) AS budget_version_hk
    ,budget_line_hk::VARCHAR(40) AS budget_line_hk
    FROM {{ source('dv', 'link_budget_version__budget_line') }}
)
SELECT * FROM source_data