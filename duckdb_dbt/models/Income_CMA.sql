-- models/fin_neighborhood_incomes.sql
{{ config(materialized='table') }}


SELECT * FROM {{ ref('stg_Income_CMA') }}

