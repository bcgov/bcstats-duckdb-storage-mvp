-- models/staging/stg_statscan_census2.sql

SELECT *
FROM read_csv_auto("{{ target.external_root }}/98-401-X2021006_English_CSV_data_BritishColumbia-utf8.csv")


