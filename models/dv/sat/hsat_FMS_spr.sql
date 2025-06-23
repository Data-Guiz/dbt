{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
    SELECT
        fms_spr_hk::VARCHAR(40) AS fms_spr_hk
        ,load_dts::TIMESTAMP AS load_dts
        ,load_end_dts::TIMESTAMP AS load_end_dts
        ,del_flag::BOOLEAN AS del_flag
        ,hash_diff::VARCHAR(40) AS hash_diff
        ,record_source::VARCHAR(40) AS record_source
        ,dv_run_id::INTEGER AS dv_run_id
        ,spr_transdate::DATE AS spr_transdate
        ,spr_dep_id::BIGINT AS spr_dep_id
        ,spr_description::VARCHAR(255) AS spr_description
        ,spr_discount::INTEGER AS spr_discount
        ,spr_discount_type::VARCHAR(1) AS spr_discount_type
        ,spr_item_no::INTEGER AS spr_item_no
        ,spr_quantity::INTEGER AS spr_quantity
        ,spr_type::VARCHAR(1) AS spr_type
        ,spr_void::BIGINT AS spr_void
    FROM {{ source('dv', 'hsat_fms_spr') }}
)
SELECT * FROM source_data