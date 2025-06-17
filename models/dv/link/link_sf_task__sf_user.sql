{{ config(
    materialized='table',
    schema='dv',
    database='dev',
    tags=['link']
) }}

WITH source_data AS (
    SELECT
    sf_task__sf_user_hk::VARCHAR(40) AS sf_task__sf_user_hk
    ,load_dts::TIMESTAMP AS load_dts
    ,record_src::VARCHAR(50) AS record_src
    ,dv_runid::INTEGER AS dv_runid
    ,sf_task_hk::VARCHAR(40) AS sf_task_hk
    ,sf_user_hk::VARCHAR(40) AS sf_user_hk
    FROM {{ source('dv', 'link_sf_task__sf_user') }}
)
SELECT * FROM source_data