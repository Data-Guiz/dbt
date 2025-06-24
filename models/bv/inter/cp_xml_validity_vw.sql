{{ 
  config(
    materialized='view',
    schema='wrk'
  ) 
}}

with xml_ordered as (
    select 
        booking_hk,
        file_name,
        cast(to_timestamp(substring(file_name, position('2' in file_name), 23), 'YYYY-MM-DD-HH24-MI-SS-MS') as timestamp) as date_with_ms,
        rank() over (partition by booking_hk order by datetime_from_file_name, file_name) as xml_rank
    from (
        select distinct
            booking_hk,
            file_name,
            datetime_from_file_name
        from {{ ref('hsat_cp_booking') }}
    )
)

select 
    xml.booking_hk,
    xml.file_name, 
    xml.date_with_ms as startdate,
    coalesce(dateadd(ms, -1, xml_next.date_with_ms), '9999-12-31 00:00:00.000') as enddate
from
    xml_ordered xml
    left join xml_ordered xml_next
        on xml.booking_hk = xml_next.booking_hk 
        and xml.xml_rank + 1 = xml_next.xml_rank