{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
        fms_epo_hk::CHAR(40) as fms_epo_hk
        ,load_dts::TIMESTAMP as load_dts
        ,record_source::VARCHAR(40) as record_source
        ,dv_run_id::INTEGER as dv_run_id
        ,fms_epo_bk::VARCHAR(40) as fms_epo_bk
    FROM {{ source('dv', 'hub_fms_epo') }}
)

SELECT 
    * 
FROM source_data