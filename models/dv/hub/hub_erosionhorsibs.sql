{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
        id_erosionhorsibs_hk::CHAR(40) as id_erosionhorsibs_hk
        ,load_dts::TIMESTAMP as load_dts
        ,record_source::VARCHAR(40) as record_source
        ,dv_run_id::INTEGER as dv_run_id
        ,id_erosionhorsibs_bk::VARCHAR(40) as id_erosionhorsibs_bk
    FROM {{ source('dv', 'hub_erosionhorsibs') }}
)

SELECT 
    * 
FROM source_data
