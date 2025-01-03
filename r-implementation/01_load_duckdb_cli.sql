-- # Copyright 2024 Province of British Columbia
-- #
-- # Licensed under the Apache License, Version 2.0 (the "License");
-- # you may not use this file except in compliance with the License.
-- # You may obtain a copy of the License at
-- #
-- # http://www.apache.org/licenses/LICENSE-2.0
-- #
-- # Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
-- # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- # See the License for the specific language governing permissions and limitations under the License.

-- Connect to your database or create a new one
.open db/bcstats_ses_1.duckdb

-- Load extensions if needed
-- .load httpfs
-- .load parquet





-- Read your CSV files
CREATE TABLE GCS_202406 AS SELECT * FROM read_csv_auto('raw_data/GCS_202406.csv');

-- https://duckdb.org/docs/data/csv/auto_detection

-- use line mode in CLI to get the full command
.mode line
SELECT Prompt FROM sniff_csv('raw_data/98-401-X2021006_English_CSV_data_BritishColumbia.csv');

-- Check the data, ignore errors: type error in some rows. This will skip lines with encoding errors.
-- or specify the column types
SELECT * FROM read_csv_auto('raw_data/98-401-X2021006_English_CSV_data_BritishColumbia.csv', ignore_errors=true) LIMIT 5;

-- * Override the type for this column manually by setting the type explicitly, e.g. types={'birth_date': 'VARCHAR'}
SELECT * FROM read_csv_auto('raw_data/98-401-X2021006_English_CSV_data_BritishColumbia.csv', types={'GEO_NAME': 'VARCHAR', 'CHARACTERISTIC_NAME': 'VARCHAR'}) LIMIT 5;


-- Another option is to use the NULL_IF parameter to replace problematic values with NULL:
-- SELECT * FROM read_csv_auto('raw_data/98-401-X2021006_English_CSV_data_BritishColumbia.csv', NULL_IF='*') LIMIT 5;










-- Identify the problematic character:
-- Try to open the CSV file in a text editor that can handle different encodings (like Notepad++ on Windows) and look at line 654, character position 77. This might give you an idea of what character is causing the issue.
-- 2021,"2021A000011124","01","Country","Canada",3.1,4.3,"20000",652,"          Edo",,1685,"",745,"",940,"",0,"",0,"",0,""
-- https://duckdb.org/docs/data/csv/reading_faulty_csv_files


-- Read the file with the correct encoding
-- SELECT * FROM read_csv_auto('raw_data/98-401-X2021006_English_CSV_data_BritishColumbia.csv', encoding='ascii') LIMIT 5;

SELECT * FROM read_csv_auto('raw_data/98-401-X2021006_English_CSV_data_BritishColumbia-utf8.csv', SAMPLE_SIZE = 1000);


-- Or read Parquet files
CREATE TABLE your_parquet_table AS SELECT * FROM parquet_scan('your_file.parquet');

-- Perform your data processing operations
CREATE TABLE result_table AS
SELECT ...
FROM your_table
WHERE ...
GROUP BY ...;

-- Export results if needed
COPY result_table TO 'output.csv' (HEADER, DELIMITER ',');

-- Or save as Parquet
COPY result_table TO 'output.parquet' (FORMAT PARQUET);
