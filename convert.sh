head -n 5 raw_data/98-401-X2021006_English_CSV_data_BritishColumbia.csv

# -- Check the encoding of the file
file -i raw_data/98-401-X2021006_English_CSV_data_BritishColumbia.csv

# -- iconv -f ASCII  -t UTF-8 raw_data/98-401-X2021006_English_CSV_data_BritishColumbia.csv > raw_data/98-401-X2021006_English_CSV_data_BritishColumbia-utf8.csv

# -- This command will:
# -- Attempt to convert from ASCII to UTF-8
# -- Use //TRANSLIT to replace characters that can't be represented in UTF-8
# -- Use -c to skip invalid characters

iconv -f ASCII -t UTF-8//TRANSLIT -c raw_data/98-401-X2021006_English_CSV_data_BritishColumbia.csv > raw_data/98-401-X2021006_English_CSV_data_BritishColumbia-utf8.csv

iconv -f ASCII -t UTF-8//TRANSLIT -c raw_data/98-401-X2021025_English_CSV_data.csv > raw_data/98-401-X2021025_English_CSV_data-utf8.csv

dbt run
dbt test