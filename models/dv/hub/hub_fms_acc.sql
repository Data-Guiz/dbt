{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
        fms_acc_hk::CHAR(40) as fms_acc_hk
        ,load_dts::TIMESTAMP as load_dts
        ,record_source::VARCHAR(40) as record_source
        ,dv_run_id::INTEGER as dv_run_id
        ,fms_acc_bk::VARCHAR(40) as fms_acc_bk
    FROM {{ source('dv', 'hub_fms_acc') }}
)

SELECT 
    * 
FROM source_data