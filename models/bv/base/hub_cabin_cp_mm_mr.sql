{{
    config(
        materialized='table'
    )
}}

SELECT * FROM {{ source('dv', 'hub_cabin_cp__mm__mr') }} 