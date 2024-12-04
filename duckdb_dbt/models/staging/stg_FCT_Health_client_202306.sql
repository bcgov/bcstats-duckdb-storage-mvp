-- models/staging/FCT_Health_client_202306.sql


SELECT STUDY_ID,	BIRTH_YR_MON,	SEX,	POSTAL_CODE,	CITY,	STREET_LINE,	LHA,
'2023-06-01' AS EFF_DATE,  '2023-06-30' as END_DATE
FROM read_csv_auto("{{ target.external_root }}/CLR_EXT_20230726.csv")
