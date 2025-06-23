{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
	SELECT
		budget_pickup_hk::CHAR(40) AS budget_pickup_hk
		,load_dts::TIMESTAMP AS load_dts
		,load_end_dts::TIMESTAMP AS load_end_dts
		,del_flag::BOOLEAN AS del_flag
		,hash_diff::VARCHAR(40) AS hash_diff
		,record_source::VARCHAR(40) AS record_source
		,dv_run_id::INTEGER AS dv_run_id
		,date_obs::TIMESTAMP AS date_obs
		,week_obs::VARCHAR(255) AS week_obs
		,n_departure::VARCHAR(255) AS n_departure
		,source_pax_3::VARCHAR(255) AS source_pax_3
		,source_pax_2::VARCHAR(255) AS source_pax_2
		,vessel_category::VARCHAR(255) AS vessel_category
		,bkg_direct_agency::VARCHAR(255) AS bkg_direct_agency
		,pax_cruise_paying_ytd::DOUBLE PRECISION AS pax_cruise_paying_ytd
		,pax_cruise_paying_weekly::DOUBLE PRECISION AS pax_cruise_paying_weekly
		,ntr_cruise_proforma_ytd::DOUBLE PRECISION AS ntr_cruise_proforma_ytd
		,ntr_cruise_proforma_weekly::DOUBLE PRECISION AS ntr_cruise_proforma_weekly
		,pcd_paying_ytd::DOUBLE PRECISION AS pcd_paying_ytd
		,pcd_paying_weekly::DOUBLE PRECISION AS pcd_paying_weekly
	FROM {{ source('dv', 'hsat_rm_budget_pickup') }}
)
SELECT * FROM source_data