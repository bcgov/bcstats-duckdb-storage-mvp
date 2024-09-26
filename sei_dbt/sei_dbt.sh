#!/bin/bash

# -- This command will:
# -- Attempt to convert from ASCII to UTF-8
# -- Use //TRANSLIT to replace characters that can't be represented in UTF-8
# -- Use -c to skip invalid characters

iconv -f ASCII -t UTF-8//TRANSLIT -c raw_data/98-401-X2021006_English_CSV_data_BritishColumbia.csv > raw_data/98-401-X2021006_English_CSV_data_BritishColumbia-utf8.csv

iconv -f ASCII -t UTF-8//TRANSLIT -c raw_data/98-401-X2021025_English_CSV_data.csv > raw_data/98-401-X2021025_English_CSV_data-utf8.csv
# Run dbt
dbt run
