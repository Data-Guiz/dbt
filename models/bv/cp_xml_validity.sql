{{
    config(
        materialized='table',
        schema='bv'
    )
}}

WITH source_data AS (
    SELECT 
        booking_hk::CHAR(40) AS booking_hk,
        file_name::VARCHAR(100) AS file_name,
        startdate::TIMESTAMP AS startdate,
        enddate::TIMESTAMP AS enddate
    FROM {{ source('bv', 'cp_xml_validity') }}
)

SELECT * FROM source_data
