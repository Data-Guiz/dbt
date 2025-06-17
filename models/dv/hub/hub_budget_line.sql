{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
        budget_line_hk::CHAR(40) AS sf_account_hk
        ,load_dts::TIMESTAMP AS load_dts
        ,record_source::VARCHAR(40) AS record_source
        ,dv_run_id::INTEGER AS dv_run_id
        ,budget_line_bk::VARCHAR(40) AS budget_line_bk
    FROM {{ source('dv', 'hub_budget_line') }}
)
SELECT 
    * 
FROM source_data 