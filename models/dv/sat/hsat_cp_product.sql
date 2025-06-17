{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
        cruise_hk::CHAR(40) AS cruise_hk,
        load_dts::TIMESTAMP AS load_dts,
        load_end_dts::TIMESTAMP AS load_end_dts,
        del_flag::BOOLEAN AS del_flag,
        hash_diff::VARCHAR(40) AS hash_diff,
        record_source::VARCHAR(40) AS record_source,
        dv_run_id::INTEGER AS dv_run_id,
        file_name::VARCHAR(100) AS file_name,
        datetime_from_file_name::TIMESTAMP AS datetime_from_file_name,
        controllingoffice::VARCHAR(100) AS controllingoffice,
        ischartedcruise::VARCHAR(3) AS ischartedcruise,
        embarkdate::VARCHAR(8) AS embarkdate,
        embarkdate__date::DATE AS embarkdate__date,
        disembarkdate::VARCHAR(8) AS disembarkdate,
        disembarkdate__date::DATE AS disembarkdate__date,
        region::VARCHAR(100) AS region,
        cruiseid::VARCHAR(100) AS cruiseid,
        shipcode::VARCHAR(2) AS shipcode,
        embarkat::VARCHAR(100) AS embarkat,
        disembarkat::VARCHAR(100) AS disembarkat,
        configurationcode::VARCHAR(100) AS configurationcode,
        cruisetype::VARCHAR(100) AS cruisetype,
        technicalincidenttype::VARCHAR(100) AS technicalincidenttype,
        technicalincidentcriticity::VARCHAR(100) AS technicalincidentcriticity,
        season::VARCHAR(100) AS season,
        vip::VARCHAR(3) AS vip,
        vipname::VARCHAR(100) AS vipname,
        expedition::VARCHAR(3) AS expedition,
        controlcapacity::VARCHAR(100) AS controlcapacity,
        prepostsalesopeningdate::VARCHAR(100) AS prepostsalesopeningdate,
        prepostsalesopeningdate__date::DATE AS prepostsalesopeningdate__date,
        excsalesopeningdate::VARCHAR(100) AS excsalesopeningdate,
        excsalesopeningdate__date::DATE AS excsalesopeningdate__date,
        status::VARCHAR(100) AS status
    FROM {{ source('dv', 'hsat_cp_product') }}
)

SELECT * FROM source_data 