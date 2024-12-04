{{ config(materialized='table') }}
-- Read data from a staging table
SELECT * FROM {{ ref('stg_bca_folio_addresses_20240825_WEEKLY') }}
