{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
        mm_cruise_hk::CHAR(40) AS mm_cruise_hk,
        load_dts::TIMESTAMP AS load_dts,
        load_end_dts::TIMESTAMP AS load_end_dts,
        del_flag::BOOLEAN AS del_flag,
        hash_diff::VARCHAR(40) AS hash_diff,
        record_source::VARCHAR(40) AS record_source,
        dv_run_id::INTEGER AS dv_run_id,
        id::INTEGER AS id,
        uid::VARCHAR(255) AS uid,
        editor::VARCHAR(50) AS editor,
        creation_timestamp::TIMESTAMP AS creation_timestamp,
        edition_timestamp::TIMESTAMP AS edition_timestamp,
        destination::VARCHAR(50) AS destination,
        from_to::VARCHAR(100) AS from_to,
        destination_trimester::VARCHAR(50) AS destination_trimester,
        vessel::VARCHAR(30) AS vessel,
        ship_class::VARCHAR(30) AS ship_class,
        year_departure::INTEGER AS year_departure,
        date_departure::DATE AS date_departure,
        los::INTEGER AS los,
        analyst::VARCHAR(100) AS analyst,
        clx_fee::INTEGER AS clx_fee,
        cruise_type::VARCHAR(30) AS cruise_type,
        bord_pref::VARCHAR(30) AS bord_pref,
        charter::VARCHAR(1) AS charter,
        cruise_status::VARCHAR(30) AS cruise_status,
        capacity_pax::INTEGER AS capacity_pax,
        season::VARCHAR(20) AS season,
        destination_trimester_corpo::VARCHAR(30) AS destination_trimester_corpo,
        theme::VARCHAR(250) AS theme,
        guest::VARCHAR(100) AS guest,
        partnership::VARCHAR(100) AS partnership,
        itinerary::VARCHAR(10) AS itinerary,
        constraint_level::VARCHAR(10) AS constraint_level,
        cluster_pricing::VARCHAR(255) AS cluster_pricing,
        eligible_us_to::INTEGER AS eligible_us_to,
        charter_name::VARCHAR(255) AS charter_name,
        dte_debut_validite::DATE AS dte_debut_validite,
        dte_fin_validite::DATE AS dte_fin_validite,
        fcst_cluster_pkl::VARCHAR(255) AS fcst_cluster_pkl,
        fcst_cluster::VARCHAR(255) AS fcst_cluster,
        fcst_model::VARCHAR(255) AS fcst_model,
        need_date_level::INTEGER AS need_date_level,
        destination_production::VARCHAR(255) AS destination_production,
        destination_semester::VARCHAR(255) AS destination_semester,
        air_manager::VARCHAR(255) AS air_manager,
        date_sales_open::DATE AS date_sales_open,
        cluster_pricing_2::VARCHAR(255) AS cluster_pricing_2,
        premium_pricing::VARCHAR(255) AS premium_pricing,
        period_pricing::VARCHAR(255) AS period_pricing,
        day_at_sea::INTEGER AS day_at_sea,
        "map"::VARCHAR(255) AS "map",
        conditions_tarifaires::VARCHAR(255) AS conditions_tarifaires,
        new_cruise::VARCHAR(255) AS new_cruise,
        cruise_code::VARCHAR(10) AS cruise_code
    FROM {{ source('dv', 'hsat_MM_cruise') }}
)

SELECT * FROM source_data 