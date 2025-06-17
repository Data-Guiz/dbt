{{
    config(
        materialized='table'
    )
}}

SELECT 
	financial_account_code
	,financial_account_label
	,dt_creation::date as dt_creation
	,dt_update::date as dt_update
FROM {{ source('dwh', 'd_financial_account') }} 