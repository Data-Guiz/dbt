{{
    config(
        materialized='table'
    )
}}
with cp_mm as (
    select
        hub_cabin.cabin_hk,
        hub_cabin.load_dts,
        hub_cabin.record_source,
        hub_cabin.dv_run_id,
        hub_cabin.cabin_bk
    from {{ ref('hub_cabin') }} as hub_cabin
),
mr as (
    select distinct
        sha1(upper(btrim(hub_vessel.vessel_bk || cabin.cabin_no))) as cabin_hk,
        cabin.load_dts,
        'MR__Cabin_Recalculated' as record_source,
        cabin.dv_run_id,
        hub_vessel.vessel_bk || cabin.cabin_no as cabin_bk
    from {{ ref('hub_vessel') }} as hub_vessel
    inner join (
        select
            l_vvv.vessel_hk,
            sat_cabin.cabin_no,
            hub_cabin.* 
        from {{ ref('link_vessel__mr_vessel__mr_vessel_type') }} l_vvv
        inner join {{ ref('link_configuration__mr_vessel__mr_vessel_configuration') }} l_cvv
            on l_cvv.mr_vessel_hk = l_vvv.mr_vessel_hk
        inner join {{ ref('link_cabincat__mr_cabin_number__mr_vesconfig') }} l_ccv
            on l_ccv.mr_vessel_configuration_hk = l_cvv.mr_vessel_configuration_hk
        inner join {{ ref('hub_mr_cabin_number') }} hub_cabin
            on hub_cabin.mr_cabin_number_hk = l_ccv.mr_cabin_number_hk
        inner join {{ ref('hsat_mr_cabin_number') }} sat_cabin
            on sat_cabin.mr_cabin_number_hk = l_ccv.mr_cabin_number_hk
            and sat_cabin.load_end_dts = '9999-12-31'
            and sat_cabin.del_flag = false
    ) cabin
        on cabin.vessel_hk = hub_vessel.vessel_hk
)
select * from cp_mm
union
select * from mr