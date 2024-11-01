-- models/staging/FCT_Health_client_202302.sql


SELECT STUDY_ID,	BIRTH_YR_MON,	SEX,	POSTAL_CODE,	CITY,	STREET_LINE,	LHA,
'2023-01-01' AS EFF_DATE,  '2023-01-31' as END_DATE
FROM read_csv_auto("{{ target.external_root }}/BC Stat Population Estimates_20230227.csv")
