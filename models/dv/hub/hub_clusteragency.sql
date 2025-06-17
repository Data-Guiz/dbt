{{
    config(
        materialized='table',
        schema='dv',
        dist='clusteragency_hk',
        sort=['clusteragency_hk', 'clusteragency_bk']
    )
}}

WITH source_data AS (
    SELECT
        clusteragency_hk::CHAR(40) as clusteragency_hk
        ,load_dts::TIMESTAMP as load_dts
        ,record_source::VARCHAR(40) as record_source
        ,dv_run_id::INTEGER as dv_run_id
        ,clusteragency_bk::VARCHAR(40) as clusteragency_bk
    FROM {{ source('dv', 'hub_clusteragency') }}
)

SELECT 
    * 
FROM source_data