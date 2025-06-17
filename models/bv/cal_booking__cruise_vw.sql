{{
    config(
        materialized='view',
        schema='bv'
    )
}}

WITH source_data AS (
    SELECT DISTINCT 
        lsat.booking_hk,
        lsat.cruise_hk,
        lsat.file_name,
        lsat.date_departure,
        lsat.date_arrival,
        lsat.itemstatuscode,
        ROW_NUMBER() OVER(
            PARTITION BY lsat.booking_hk, lsat.file_name
            ORDER BY lsat.itemstatuscode, lsat.date_departure, lsat.cruise_bk
        ) AS n_order_cruise,
        CASE
            WHEN (ROW_NUMBER() OVER(
                PARTITION BY lsat.booking_hk, lsat.file_name, lsat.itemstatuscode
                ORDER BY lsat.date_departure, lsat.cruise_bk
            )) = 1 THEN 1
            ELSE 0
        END AS fg_first_cruise,
        CASE
            WHEN (ROW_NUMBER() OVER(
                PARTITION BY lsat.booking_hk, lsat.file_name, lsat.itemstatuscode
                ORDER BY lsat.date_departure DESC, lsat.cruise_bk DESC
            )) = 1 THEN 1
            ELSE 0
        END AS fg_last_cruise
    FROM {{ source('wrk', 'lsat_cp_booking_cruise_vw') }} lsat
)

SELECT * FROM source_data 