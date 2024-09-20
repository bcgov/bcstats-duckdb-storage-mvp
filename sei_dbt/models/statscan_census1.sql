-- models/fin_neighborhood_incomes.sql
{{ config(materialized='table') }}

SELECT 
        *
        -- Add any other transformations here
FROM {{ ref('stg_statscan_census1') }}
