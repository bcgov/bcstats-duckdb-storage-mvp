-- models/staging/stg_fin_income_2021.sql
{{ config(materialized='table') }}

SELECT tax_year, postal_code, cast(median_income AS DOUBLE) AS median_income FROM {{ source('external_source', 'fin_neighborhood_incomes_2021') }}
WHERE median_income IS NOT NULL AND median_income <> '1.00E+05'


