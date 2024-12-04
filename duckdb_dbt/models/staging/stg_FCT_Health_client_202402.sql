-- models/staging/FCT_Health_client_202402.sql


SELECT STUDY_ID,	BIRTH_YR_MON,	SEX,	POSTAL_CODE,	CITY,	STREET_LINE,	LHA,
EFF_DATE, case when END_DATE IS NULL then '2024-02-29' else END_DATE end as END_DATE
FROM read_csv_auto("{{ target.external_root }}/BC Stat Population Estimates_20240326.csv")
