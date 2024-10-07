{{ config(materialized = 'external', location = 'output/fin_neighborhood_incomes_2021_5.csv', format = 'csv') }}

SELECT * FROM {{ source('external_source', 'fin_neighborhood_incomes_2021') }} fni LIMIT 5