{{
    config(
        materialized='table',
        schema='dv',
        dist='budget_pickup_hk',
        sort=['budget_pickup_hk', 'budget_pickup_bk']
    )
}}

WITH source_data AS (
    SELECT
        budget_pickup_hk::CHAR(40) as budget_pickup_hk
        ,load_dts::TIMESTAMP as load_dts
        ,record_source::VARCHAR(40) as record_source
        ,dv_run_id::INTEGER as dv_run_id
        ,budget_pickup_bk::VARCHAR(40) as budget_pickup_bk
    FROM {{ source('dv', 'hub_budget_pickup') }}
)

SELECT 
    * 
FROM source_data 
