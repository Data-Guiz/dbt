{{
    config(
        materialized='table'
    )
}}

SELECT 
*
FROM {{ source('bv', 'cp_xml_validity') }}