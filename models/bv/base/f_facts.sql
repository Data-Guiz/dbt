{{
    config(
        materialized='table'
    )
}}

SELECT * 
FROM {{ source('dm', 'f_facts') }} 