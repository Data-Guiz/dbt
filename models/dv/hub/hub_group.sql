{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
        group_hk::CHAR(40) as group_hk
        ,load_dts::TIMESTAMP as load_dts
        ,record_source::VARCHAR(40) as record_source
        ,dv_run_id::INTEGER as dv_run_id
        ,group_bk::VARCHAR(40) as group_bk
    FROM {{ source('dv', 'hub_group') }}
)

SELECT 
    * 
FROM source_data

