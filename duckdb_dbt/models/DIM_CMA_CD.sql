-- models/fin_neighborhood_incomes.sql
{{ config(materialized='table') }}


SELECT * FROM {{ ref('stg_DIM_CMA_CD') }}

