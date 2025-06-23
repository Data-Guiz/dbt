{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
    SELECT
        fms_pos_hk::VARCHAR(40) AS fms_pos_hk
        ,load_dts::TIMESTAMP AS load_dts
        ,load_end_dts::TIMESTAMP AS load_end_dts
        ,del_flag::BOOLEAN AS del_flag
        ,hash_diff::VARCHAR(40) AS hash_diff
        ,record_source::VARCHAR(40) AS record_source
        ,dv_run_id::INTEGER AS dv_run_id
        ,pos_trans_id::BIGINT AS pos_trans_id
        ,pos_mib_id::BIGINT AS pos_mib_id
        ,pos_closed::INTEGER AS pos_closed
        ,pos_win::INTEGER AS pos_win
        ,pos_usr_id::BIGINT AS pos_usr_id
        ,pos_sdat::TIMESTAMP AS pos_sdat
        ,pos_pdat::TIMESTAMP AS pos_pdat
        ,pos_buyer::BIGINT AS pos_buyer
        ,pos_pay_o::BIGINT AS pos_pay_o
        ,pos_payer::BIGINT AS pos_payer
        ,pos_dis_per::DOUBLE PRECISION AS pos_dis_per
        ,pos_transdate::TIMESTAMP AS pos_transdate
        ,pos_fleet_id::INTEGER AS pos_fleet_id
        ,pos_dep_desc::VARCHAR(50) AS pos_dep_desc
        ,pos_dem_id::BIGINT AS pos_dem_id
        ,pos_dem_desc::VARCHAR(50) AS pos_dem_desc
        ,pos_dem_class_id::VARCHAR(10) AS pos_dem_class_id
        ,pos_dem_class_desc::VARCHAR(50) AS pos_dem_class_desc
        ,pos_dep_class_id::VARCHAR(10) AS pos_dep_class_id
        ,pos_dep_class_desc::VARCHAR(50) AS pos_dep_class_desc
        ,pos_noprint::INTEGER AS pos_noprint
        ,pos_dep_no::VARCHAR(10) AS pos_dep_no
        ,pos_pay_i::DOUBLE PRECISION AS pos_pay_i
        ,pos_value::DOUBLE PRECISION AS pos_value
        ,pos_quant::INTEGER AS pos_quant
        ,pos_void::DOUBLE PRECISION AS pos_void
        ,pos_dep_id::BIGINT AS pos_dep_id
        ,pos_bill_no::VARCHAR(20) AS pos_bill_no
        ,pos_uniq_key::VARCHAR(32) AS pos_uniq_key	
    FROM {{ source('dv', 'hsat_fms_pos') }}
)
SELECT * FROM source_data