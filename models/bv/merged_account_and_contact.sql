{{
    config(
        materialized='table',
        schema='bv'
    )
}}

WITH source_data AS (
    SELECT DISTINCT
        ACCOUNTID_OR_CONTACTID,
        ACCOUNTID_OR_CONTACTID_POST_MERGE,
        ACCOUNT_OR_CONTACT_HK,
        ACCOUNT_OR_CONTACT_POST_MERGE_HK,
        TYPE_OF_OBJECT_POST_MERGE
    FROM {{ source('dv', 'merged_account_and_contact') }}
)

SELECT * FROM source_data 