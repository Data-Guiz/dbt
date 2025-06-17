{{
    config(
        materialized='table',
        schema='dv',
        dist='cruise_cluster_hk',
        sort=['cruise_cluster_hk', 'cruise_cluster_bk']
    )
}}

WITH source_data AS (
    SELECT
        cruise_cluster_hk::CHAR(40) as cruise_cluster_hk
        ,load_dts::TIMESTAMP as load_dts
        ,record_source::VARCHAR(40) as record_source
        ,dv_run_id::INTEGER as dv_run_id
        ,cruise_cluster_bk::VARCHAR(40) as cruise_cluster_bk
    FROM {{ source('dv', 'hub_cruise_cluster') }}
)

SELECT 
    * 
FROM source_data
