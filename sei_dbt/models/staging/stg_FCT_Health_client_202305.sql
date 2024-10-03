-- models/staging/FCT_Health_client_202305.sql


SELECT STUDY_ID,	BIRTH_YR_MON,	SEX,	POSTAL_CODE,	CITY,	STREET_LINE,	LHA,
'2023-05-01' AS EFF_DATE,  '2023-05-31' as END_DATE
FROM read_csv_auto("{{ target.external_root }}/CLR_EXT_20230626.csv")
