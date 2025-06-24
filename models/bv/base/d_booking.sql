{{
    config(
        materialized='table'
    )
}}

SELECT * 
FROM {{ source('dm', 'd_booking') }} 