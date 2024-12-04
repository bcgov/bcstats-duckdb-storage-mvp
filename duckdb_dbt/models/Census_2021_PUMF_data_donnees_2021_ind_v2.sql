-- models/fin_neighborhood_incomes.sql
{{ config(materialized='table') }}


SELECT * FROM {{ ref('stg_Census_2021_PUMF_data_donnees_2021_ind_v2') }}

