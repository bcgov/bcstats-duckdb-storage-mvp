-- models/staging/FCT_Health_client_202405.sql
{{ config(materialized='table') }}

SELECT STUDY_ID,	BIRTH_YR_MON,	SEX,	POSTAL_CODE,	CITY,	STREET_LINE,	LHA,
EFF_DATE, case when END_DATE IS NULL then '2024-06-28' else END_DATE end as END_DATE
FROM read_csv_auto("C:/Users/JDUAN/OneDrive - Government of BC/2024-025 Brett and Jon Database Test Warehouse/raw_data/BC Stat Population Estimates_20240628.csv")
