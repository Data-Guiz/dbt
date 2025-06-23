{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
    SELECT
        fms_sscd_hk::VARCHAR(40) AS fms_sscd_hk
        ,load_dts::TIMESTAMP AS load_dts
        ,load_end_dts::TIMESTAMP AS load_end_dts
        ,del_flag::BOOLEAN AS del_flag
        ,hash_diff::VARCHAR(40) AS hash_diff
        ,record_source::VARCHAR(40) AS record_source
        ,dv_run_id::INTEGER AS dv_run_id
        ,sscd_shp_id::INTEGER AS sscd_shp_id
        ,sscd_detail_line::INTEGER AS sscd_detail_line
        ,sscd_item_id::INTEGER AS sscd_item_id
        ,sscd_item_description::VARCHAR(136) AS sscd_item_description
        ,sscd_sold::INTEGER AS sscd_sold
        ,sscd_price::DOUBLE PRECISION AS sscd_price
        ,sscd_total::DOUBLE PRECISION AS sscd_total
        ,sscd_type::INTEGER AS sscd_type
        ,sscd_deducted::INTEGER AS sscd_deducted
        ,sscd_transdate::TIMESTAMP AS sscd_transdate
        ,sscd_mainlevel::INTEGER AS sscd_mainlevel
        ,sscd_pricelevel::INTEGER AS sscd_pricelevel
        ,sscd_remarks::VARCHAR(128) AS sscd_remarks
        ,sscd_sublevel::INTEGER AS sscd_sublevel
        ,sscd_defseq::INTEGER AS sscd_defseq
        ,sscd_item_type::VARCHAR(1) AS sscd_item_type
        ,sscd_si::INTEGER AS sscd_si
    FROM {{ source('dv', 'hsat_fms_sscd') }}
)
SELECT * FROM source_data