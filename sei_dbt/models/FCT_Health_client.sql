-- models/fin_neighborhood_incomes.sql
{{ config(materialized='table') }}


SELECT * FROM {{ ref('stg_FCT_Health_client_202304') }}
UNION ALL
SELECT * FROM {{ ref('stg_FCT_Health_client_202305') }}
UNION ALL
SELECT * FROM {{ ref('stg_FCT_Health_client_202306') }}
UNION ALL
SELECT * FROM {{ ref('stg_FCT_Health_client_202307') }}
UNION ALL
SELECT * FROM {{ ref('stg_FCT_Health_client_202308') }}
UNION ALL
SELECT * FROM {{ ref('stg_FCT_Health_client_202309') }}
UNION ALL
SELECT * FROM {{ ref('stg_FCT_Health_client_202310') }}
UNION ALL
SELECT * FROM {{ ref('stg_FCT_Health_client_202311') }}
UNION ALL
SELECT * FROM {{ ref('stg_FCT_Health_client_202400') }}
UNION ALL
SELECT * FROM {{ ref('stg_FCT_Health_client_202401') }}
UNION ALL
SELECT * FROM {{ ref('stg_FCT_Health_client_202402') }}
UNION ALL
SELECT * FROM {{ ref('stg_FCT_Health_client_202403') }}
UNION ALL
SELECT * FROM {{ ref('stg_FCT_Health_client_202404') }}
UNION ALL
SELECT * FROM {{ ref('stg_FCT_Health_client_202405') }}
UNION ALL
SELECT * FROM {{ ref('stg_FCT_Health_client_202406') }}
UNION ALL
SELECT * FROM {{ ref('stg_FCT_Health_client_202407') }}
