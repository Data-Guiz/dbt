{{
    config(
        materialized='table'
    )
}}

Select *
From {{ source('bv', 'cp_xml_validity') }}