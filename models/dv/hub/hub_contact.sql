{{
    config(
        materialized='table',
        schema='dv',
        dist='contact_hk',
        sort=['contact_hk', 'contact_bk']
    )
}}

WITH source_data AS (
    SELECT
        contact_hk::CHAR(40) as contact_hk
        ,load_dts::TIMESTAMP as load_dts
        ,record_source::VARCHAR(40) as record_source
        ,dv_run_id::INTEGER as dv_run_id
        ,contact_bk::VARCHAR(40) as contact_bk
    FROM {{ source('dv', 'hub_contact') }}
)

SELECT 
    * 
FROM source_data