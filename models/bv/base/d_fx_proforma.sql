{{
    config(
        materialized='table'
    )
}}

SELECT * FROM {{ source('dm', 'd_fx_proforma') }} 