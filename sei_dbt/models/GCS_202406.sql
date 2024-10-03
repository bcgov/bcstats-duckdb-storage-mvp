-- models/fin_neighborhood_incomes.sql
{{ config(materialized='table') }}


SELECT * FROM {{ ref('stg_GCS_202406') }}

