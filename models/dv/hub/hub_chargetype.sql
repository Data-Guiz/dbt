{{
    config(
        materialized='table',
        schema='dv',
        dist='chargetype_hk',
        sort=['chargetype_hk', 'chargetype_bk']
    )
}}

WITH source_data AS (
    SELECT
        chargetype_hk::CHAR(40) as chargetype_hk
        ,load_dts::TIMESTAMP as load_dts
        ,record_source::VARCHAR(40) as record_source
        ,dv_run_id::INTEGER as dv_run_id
        ,chargetype_bk::VARCHAR(40) as chargetype_bk
    FROM {{ source('dv', 'hub_chargetype') }}
)

SELECT 
    * 
FROM source_data