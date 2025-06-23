{{ config(
    materialized='incremental',
    unique_key=['booking_hk', 'file_name'],
    schema='bv',
    alias='cp_xml_validity'
) }}

with source as (
    select
        booking_hk,
        file_name,
        startdate,
        enddate
    from {{ ref('cp_xml_validity_vw') }}
)

select
    booking_hk,
    file_name,
    startdate,
    enddate
from source

{% if is_incremental() %}
    -- Only insert new or updated records
    where (booking_hk, file_name, enddate) not in (
        select booking_hk, file_name, enddate from {{ this }}
    )
{% endif %}