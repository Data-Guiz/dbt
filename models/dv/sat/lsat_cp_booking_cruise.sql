{{
    config(
        materialized='table',
        schema='dv'
    )
}}

WITH source_data AS (
    SELECT
        booking__item__cruise__vessel__cabin_hk,
        load_dts,
        load_end_dts,
        del_flag,
        hash_diff,
        record_source,
        dv_run_id,
        file_name,
        datetime_from_file_name,
        itemstatuscode,
        itemstatusdesc,
        cruiseid,
        shipname,
        shipgraphicsurl,
        sailingdate,
        sailingdate__date,
        sailingtime,
        sailingcountry,
        sailingport,
        stdsailingport,
        returndate,
        returndate__date,
        returncountry,
        returnport,
        stdreturnport,
        sailinglengthdays,
        itinerarycode,
        itinerarydescription,
        categorycode,
        categorydesc,
        promotioncode,
        promotiontype,
        promotiondesc,
        waitlistcategorycode,
        waitlistcabinno,
        bookedpromotionamount,
        brochureprice,
        amountsavedbychossingpromo
    FROM {{ source('dv', 'lsat_cp_booking_cruise') }}
)

SELECT * FROM source_data 