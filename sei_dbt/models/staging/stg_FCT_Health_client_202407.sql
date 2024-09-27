-- models/staging/FCT_Health_client_202407.sql
{{ config(materialized='table') }}

SELECT STUDY_ID,	BIRTH_YR_MON,	SEX,	POSTAL_CODE,	CITY,	STREET_LINE,	LHA,
EFF_DATE, case when END_DATE IS NULL then '2024-08-27' else END_DATE end as END_DATE
FROM read_csv_auto("raw_data/BC Stat Population Estimates_20240827.csv")
