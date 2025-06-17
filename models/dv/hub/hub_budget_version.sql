{{
    config(
        materialized='table',
        schema='dv',
        dist='budget_version_hk',
        sort=['budget_version_hk', 'budget_version_bk']
    )
}}

WITH source_data AS (
    SELECT
        budget_version_hk::CHAR(40) as budget_version_hk
        ,load_dts::TIMESTAMP as load_dts
        ,record_source::VARCHAR(40) as record_source
        ,dv_run_id::INTEGER as dv_run_id
        ,budget_version_bk::VARCHAR(40) as budget_version_bk
    FROM {{ source('dv', 'hub_budget_version') }}
)

SELECT 
    * 
FROM source_data