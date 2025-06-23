{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
    SELECT
        fms_ssci_hk::VARCHAR(40) AS fms_ssci_hk
        ,load_dts::TIMESTAMP AS load_dts
        ,load_end_dts::TIMESTAMP AS load_end_dts
        ,del_flag::BOOLEAN AS del_flag
        ,hash_diff::VARCHAR(40) AS hash_diff
        ,record_source::VARCHAR(40) AS record_source
        ,dv_run_id::INTEGER AS dv_run_id
        ,ssci_check_id::INTEGER AS ssci_check_id
        ,ssci_check_status::INTEGER AS ssci_check_status
        ,ssci_xrvc_code::INTEGER AS ssci_xrvc_code
        ,ssci_check_date::TIMESTAMP AS ssci_check_date
        ,ssci_open_date::TIMESTAMP AS ssci_open_date
        ,ssci_close_date::TIMESTAMP AS ssci_close_date
        ,ssci_system_date::DATE AS ssci_system_date
        ,ssci_server_id::INTEGER AS ssci_server_id
        ,ssci_server_name::VARCHAR(80) AS ssci_server_name
        ,ssci_trans_server_id::INTEGER AS ssci_trans_server_id
        ,ssci_trans_server_name::VARCHAR(80) AS ssci_trans_server_name
        ,ssci_guest_count::INTEGER AS ssci_guest_count
        ,ssci_payment_type::INTEGER AS ssci_payment_type
        ,ssci_account_id::BIGINT AS ssci_account_id
        ,ssci_total_check::INTEGER AS ssci_total_check
        ,ssci_offline_posting::INTEGER AS ssci_offline_posting
        ,ssci_table_number::INTEGER AS ssci_table_number
        ,ssci_track::VARCHAR(60) AS ssci_track
        ,ssci_guid::VARCHAR(30) AS ssci_guid
        ,ssci_transdate::TIMESTAMP AS ssci_transdate
        ,ssci_cruise::BIGINT AS ssci_cruise
    FROM {{ source('dv', 'hsat_fms_ssci') }}
)
SELECT * FROM source_data