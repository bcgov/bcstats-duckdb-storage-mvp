-- models/staging/stg_statscan_census1.sql
{{ config(materialized='table') }}

    SELECT
        -- Convert ASCII columns to UTF-8 here
        CAST(GEO_NAME AS VARCHAR) AS GEO_NAME,
        CAST(CHARACTERISTIC_NAME AS VARCHAR) AS CHARACTERISTIC_NAME,
        -- ... other columns ...
        CAST(CENSUS_YEAR AS INTEGER) AS CENSUS_YEAR,
        CAST(DGUID AS VARCHAR) AS DGUID,
        CAST(ALT_GEO_CODE AS VARCHAR) AS ALT_GEO_CODE,
        CAST(GEO_LEVEL AS VARCHAR) AS GEO_LEVEL,
        CAST(TNR_SF AS FLOAT8) AS TNR_SF,
        CAST(TNR_LF AS FLOAT8 ) AS TNR_LF,
        CAST(DATA_QUALITY_FLAG AS VARCHAR) AS DATA_QUALITY_FLAG,
        CAST(CHARACTERISTIC_ID AS INTEGER) AS CHARACTERISTIC_ID,
        CAST(CHARACTERISTIC_NOTE AS INTEGER) AS CHARACTERISTIC_NOTE,
        CAST(C1_COUNT_TOTAL AS FLOAT8 ) AS C1_COUNT_TOTAL,
        -- To select columns with spaces or special characters, use double quotes ("):
        CAST("C2_COUNT_MEN+" AS FLOAT8 ) AS C2_COUNT_MEN,
        CAST("C3_COUNT_WOMEN+" AS FLOAT8 ) AS C3_COUNT_WOMEN,
        CAST(C10_RATE_TOTAL AS FLOAT8 ) AS C10_RATE_TOTAL,
        CAST("C11_RATE_MEN+" AS FLOAT8 ) AS C11_RATE_MEN,
        CAST("C12_RATE_WOMEN+" AS FLOAT8 ) AS C12_RATE_WOMEN

    FROM read_csv_auto("C:/Users/JDUAN/OneDrive - Government of BC/2024-025 Brett and Jon Database Test Warehouse/raw_data/98-401-X2021025_English_CSV_data-utf8.csv")


