-- models/staging/stg_fin_income_all.sql
{{ config(materialized='table') }}

with fin_2016_2020 as (
    SELECT tax_year, postal_code, cast(median_income AS DOUBLE) AS median_income FROM {{ source('external_source', 'fin_neighborhood_incomes_2016_2020') }}
    WHERE median_income IS NOT NULL AND median_income <> '1.00E+05'
),
fin_2021 as (
    SELECT tax_year, postal_code, cast(median_income AS DOUBLE) AS median_income FROM {{ source('external_source', 'fin_neighborhood_incomes_2021') }}
    WHERE median_income IS NOT NULL AND median_income <> '1.00E+05'
)
SELECT * FROM fin_2016_2020
UNION ALL
SELECT * FROM fin_2021


