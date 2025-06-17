{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['link']
) }}

WITH source_data AS (
    SELECT
    booking__contact__followup_hk::VARCHAR(40) AS booking__contact__followup_hk
    ,load_dts::TIMESTAMP AS load_dts
    ,record_src::VARCHAR(50) AS record_src
    ,dv_runid::INTEGER AS dv_runid
    ,booking_hk::VARCHAR(40) AS booking_hk		
    ,contact_hk::VARCHAR(40) AS contact_hk
    ,file_name::VARCHAR(100) AS file_name
    ,datetime_from_file_name::TIMESTAMP AS datetime_from_file_name
    ,followdesc::VARCHAR(100) AS followdesc
    ,completiondate::VARCHAR(100) AS completiondate
    ,completiondate__date::DATE AS completiondate__date
    ,reminderdate::VARCHAR(100) AS reminderdate
    ,reminderdate__date::DATE AS reminderdate__date
    FROM {{ source('dv', 'nhl_cp_booking_participant_followup') }}
)
SELECT * FROM source_data