-- models/staging/FCT_Health_client_202311.sql
{{ config(materialized='table') }}

SELECT STUDY_ID,	BIRTH_YR_MON,	SEX,	POSTAL_CODE,	CITY,	STREET_LINE,	LHA,
'2023-05-25' AS EFF_DATE,  '2023-05-25' as END_DATE
FROM read_csv_auto("raw_data/CLR_EXT_20231227.csv")
