-- models/fin_neighborhood_incomes.sql
{{ config(materialized='table') }}

with fin_2016_2020 as (
    SELECT * FROM  {{ ref('raw_FIN_Neighborhood_Incomes_2016_2020') }}
    ),
    fin_2021 as (
    SELECT * FROM  {{ ref('raw_FIN_Neighborhood_Incomes_2021') }}
)
SELECT * FROM fin_2016_2020
UNION ALL
SELECT * FROM fin_2021
