{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
    SELECT
        currency_hk::CHAR(40) AS currency_hk
        ,load_dts::TIMESTAMP AS load_dts
        ,load_end_dts::TIMESTAMP AS load_end_dts
        ,del_flag::BOOLEAN AS del_flag
        ,hash_diff::VARCHAR(40) AS hash_diff
        ,record_source::VARCHAR(40) AS record_source
        ,dv_run_id::INTEGER AS dv_run_id
        ,lb_name::VARCHAR(100) AS lb_name
        ,fg_base_currency::INTEGER AS fg_base_currency
        ,fg_inactive::INTEGER AS fg_inactive
    FROM {{ source('dv', 'hsat_ns_currencylist') }}
)
SELECT * FROM source_data
