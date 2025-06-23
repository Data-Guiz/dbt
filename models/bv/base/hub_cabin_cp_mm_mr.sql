{{
    config(
        materialized='table'
    )
}}

SELECT * FROM {{ source('bv', 'hub_cabin_cp__mm__mr') }} 