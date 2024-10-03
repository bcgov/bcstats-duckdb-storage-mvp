-- models/fin_neighborhood_incomes.sql
{{ config(materialized='table') }}

SELECT
        *
        -- Add any other transformations here
FROM {{ ref('stg_tbl_98_401_X2021006_English_BC') }}
