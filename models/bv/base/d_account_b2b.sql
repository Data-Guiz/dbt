{{
    config(
        materialized='table'
    )
}}

SELECT * 
FROM {{ source('dm', 'd_account_b2b') }} 