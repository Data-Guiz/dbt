{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['sat']
) }}

WITH source_data AS (
    SELECT
        ns_livre_compt_hk::CHAR(40) AS ns_livre_compt_hk
        ,load_dts::TIMESTAMP AS load_dts
        ,load_end_dts::TIMESTAMP AS load_end_dts
        ,del_flag::BOOLEAN AS del_flag
        ,hash_diff::VARCHAR(40) AS hash_diff
        ,record_source::VARCHAR(40) AS record_source
        ,dv_run_id::INTEGER AS dv_run_id
        ,internalid::VARCHAR(100) AS internalid
        ,lineid::VARCHAR(100) AS lineid
        ,"account"::VARCHAR(100) AS account
        ,rank_1::VARCHAR(100) AS rank_1
        ,rank_2::VARCHAR(200) AS rank_2
        ,transactiontype::VARCHAR(100) AS transactiontype
        ,item::VARCHAR(500) AS item
        ,parent::VARCHAR(100) AS parent
        ,supplier::VARCHAR(100) AS supplier
        ,client::VARCHAR(100) AS client
        ,supplier_sageid::VARCHAR(100) AS supplier_sageid
        ,clientid::VARCHAR(100) AS clientid
        ,amount_debit::DOUBLE PRECISION AS amount_debit
        ,amount_credit::DOUBLE PRECISION AS amount_credit
        ,amount_absvalue_foreigncurrency::DOUBLE PRECISION AS amount_absvalue_foreigncurrency
        ,currency::VARCHAR(50) AS currency
        ,exchangerate::DOUBLE PRECISION AS exchangerate
        ,invoiceno::VARCHAR(100) AS invoiceno
        ,purchaseorderno::VARCHAR(100) AS purchaseorderno
        ,bookingno::VARCHAR(100) AS bookingno
        ,memo::VARCHAR(1000) AS memo
        ,sourcesystem::VARCHAR(100) AS sourcesystem
        ,period::VARCHAR(100) AS period
        ,creationdate::TIMESTAMP AS creationdate
        ,subsidiary::VARCHAR(100) AS subsidiary
        ,cruise::VARCHAR(100) AS cruise
        ,vessel::VARCHAR(100) AS vessel
        ,cruisearea::VARCHAR(100) AS cruisearea
        ,employee::VARCHAR(100) AS employee
        ,"location"::VARCHAR(100) AS location
        ,market::VARCHAR(100) AS market
        ,channel::VARCHAR(100) AS channel
        ,project::VARCHAR(500) AS project
        ,service::VARCHAR(255) AS service
    FROM {{ source('dv', 'hsat_ns_livre_compt') }}
)
SELECT * FROM source_data