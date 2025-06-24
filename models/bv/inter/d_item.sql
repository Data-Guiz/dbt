{{
    config(
        materialized='table'
    )
}}


WITH cruise_items AS (
    SELECT
        -- key
        booking.booking_bk as bkg_booking_no,
        cruise.cruise_bk as cru_cruise_id,
        item.item_bk as bkg_item_id,
        -- values            
        'CRU' as bkg_item_type,
        sat_cruise.itemstatuscode as bkg_item_status_code,
        sat_cruise.itemstatusdesc as bkg_item_status_description,
        sat_cruise.categorycode as bkg_item_code,
        sat_cruise.categorydesc as bkg_item_description,
        null as bkg_item_location_code,
        sat_cruise.bookeddate__date as bkg_item_date,
        null as bkg_item_departure_date,
        null as bkg_item_arrival_date,
        sat_cruise.cabinno as pres_cabin_no,
        COALESCE(cabin_category.cabin_category_bk, sat_cruise.categorycode) as pres_cabin_category_code,
        COALESCE(sat_cabin_cat.cabin_code_ibs_equiv, pres_cabin_category_code) as cabin_category_code_equiv,
        COALESCE(sat_cabin_cat.cabin_total_cabin_code_ibs_equiv, 1) as cabin_total_cabin_code_ibs_equiv,
        sat_cruise.partnercode as bkg_partner_code,
        sat_cabin_cat.cabin_code_pricing as Cabin_Category_Pricing,
        sat_cabin_cat.room_name as Cabin_Category_Name,
        vessel_config_cabin.Cabin_Shipboard,
        vessel_config_cabin.Cabin_Position,
        sat_cruise.CabinOccupanyCode as Cabin_Occupancy_Code,
        null as Air_Booking_Type,
        null as item_booked_date,
        null as item_fare_type,
        null as item_occupancy_room,
        null as item_Flight_Class,
        null as pnrrefno,
        null as splitpnrrefno,
        null as costamount,
        null as adult_cost,
        null as child_cost,
        null as infant_cost,
        null as senior_cost,
        null as adult_price,
        null as child_price,
        null as infant_price,
        null as senior_price,
        null as plancode,
        null as plandescription,
        -- technical
        CURRENT_TIMESTAMP as dt_insert,
        CURRENT_TIMESTAMP as dt_update,
        sat_cruise.load_dts as load_dts,
        sat_cruise.PromotionCode as cru_rate_code,
        sat_cruise.PromotionDesc as cru_rate_code_description
    FROM {{ source('dv', 'hub_booking') }} booking
    INNER JOIN {{ source('bv', 'cp_xml_validity') }} xml ON
        xml.booking_hk = booking.booking_hk
        AND xml.startdate < CURRENT_DATE
        AND xml.enddate >= CURRENT_DATE
    INNER JOIN {{ source('dv', 'link_booking__item__cruise__vessel__cabin') }} l_cruise ON 
        booking.booking_hk = l_cruise.booking_hk
    INNER JOIN {{ source('dv', 'hub_cruise') }} cruise ON 
        l_cruise.cruise_hk = cruise.cruise_hk    
    INNER JOIN {{ source('dv', 'hub_item') }} item ON 
        l_cruise.item_hk = item.item_hk    
    INNER JOIN {{ source('dv', 'hub_vessel') }} vessel ON 
        l_cruise.vessel_hk = vessel.vessel_hk
    INNER JOIN {{ source('dv', 'lsat_cp_booking_cruise') }} sat_cruise ON 
        l_cruise.booking__item__cruise__vessel__cabin_hk = sat_cruise.booking__item__cruise__vessel__cabin_hk
        AND sat_cruise.load_end_dts = '9999-12-31'
        AND sat_cruise.del_flag = false
        AND sat_cruise.file_name = xml.file_name
    INNER JOIN {{ source('dv', 'link_vessel__mr_vessel__mr_vessel_type') }} l_vvv
        ON l_vvv.vessel_hk = vessel.vessel_hk
    INNER JOIN {{ source('dv', 'hsat_mr_vessel') }} sat_vessel 
        ON sat_vessel.mr_vessel_hk = l_vvv.mr_vessel_hk 
        AND sat_vessel.load_end_dts = '9999-12-31'
        AND sat_vessel.del_flag = false 
    LEFT JOIN (
        SELECT 
            l_vvv.vessel_hk, 
            sat_vessel_conf.start_date, 
            sat_vessel_conf.end_date, 
            l_cnv.cabin_category_hk,
            l_cvv.mr_vessel_configuration_hk,  
            link_cabin.cabin_hk,
            sat_cabin_num.cabin_shipboard, 
            sat_cabin_num.cabin_position 
        FROM {{ source('dv', 'link_vessel__mr_vessel__mr_vessel_type') }} l_vvv
        INNER JOIN {{ source('dv', 'hub_vessel') }} hub_vessel 
            ON hub_vessel.vessel_hk = l_vvv.vessel_hk 
        INNER JOIN {{ source('dv', 'link_configuration__mr_vessel__mr_vessel_configuration') }} l_cvv
            ON l_vvv.mr_vessel_hk = l_cvv.mr_vessel_hk
        INNER JOIN {{ source('dv', 'hsat_mr_vessel_configuration') }} sat_vessel_conf 
            ON sat_vessel_conf.mr_vessel_configuration_hk = l_cvv.mr_vessel_configuration_hk
            AND sat_vessel_conf.load_end_dts = '9999-12-31'
            AND sat_vessel_conf.del_flag = FALSE 
        INNER JOIN {{ source('dv', 'link_cabincat__mr_cabin_number__mr_vesconfig') }} l_cnv 
            ON l_cnv.mr_vessel_configuration_hk = l_cvv.mr_vessel_configuration_hk
        INNER JOIN {{ source('dv', 'hsat_mr_cabin_number') }} sat_cabin_num
            ON sat_cabin_num.mr_cabin_number_hk = l_cnv.mr_cabin_number_hk
            AND sat_cabin_num.load_end_dts = '9999-12-31'
            AND sat_cabin_num.del_flag = FALSE 
        INNER JOIN {{ source('bv', 'hub_cabin_cp__mm__mr') }} link_cabin 
            ON link_cabin.cabin_hk = SHA1(UPPER(TRIM(hub_vessel.vessel_bk || sat_cabin_num.cabin_no)))
    ) vessel_config_cabin
        ON vessel_config_cabin.vessel_hk = vessel.vessel_hk
        AND vessel_config_cabin.cabin_hk = l_cruise.cabin_hk
        AND vessel_config_cabin.start_date <= sat_cruise.sailingdate__date
        AND vessel_config_cabin.end_date > sat_cruise.sailingdate__date
    LEFT JOIN {{ source('dv', 'hub_cabin_category') }} cabin_category
        ON cabin_category.cabin_category_hk = vessel_config_cabin.cabin_category_hk
    LEFT JOIN (
        SELECT 
            sat_cabin_cat.cabin_code_ibs_equiv,
            sat_cabin_cat.cabin_total_cabin_code_ibs_equiv,
            sat_cabin_cat.cabin_code_pricing,
            sat_cabin_cat.room_name,
            sat_cabin_cat.mr_cabin_category_hk,
            sat_cabin_cat.mr_vessel_configuration_hk
        FROM {{ source('dv', 'link_cabincat__mr_cabincat__mr_vessel_config') }} l_ccv
        INNER JOIN {{ source('dv', 'hsat_mr_cabin_category') }} sat_cabin_cat 
            ON sat_cabin_cat.mr_cabin_category_hk = l_ccv.mr_cabin_category_hk
            AND sat_cabin_cat.load_end_dts = '9999-12-31'
            AND sat_cabin_cat.del_flag = FALSE
    ) sat_cabin_cat
        ON sat_cabin_cat.mr_cabin_category_hk = cabin_category.cabin_category_hk
        AND sat_cabin_cat.mr_vessel_configuration_hk = vessel_config_cabin.mr_vessel_configuration_hk
)

SELECT * FROM cruise_items 
