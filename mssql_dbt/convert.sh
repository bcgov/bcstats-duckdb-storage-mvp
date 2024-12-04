# Copyright 2024 Province of British Columbia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.




head -n 5 raw_data/98-401-X2021006_English_CSV_data_BritishColumbia.csv
head -n 5 raw_data/mobility_status\ \(Census\ Raw\).csv

# -- Check the encoding of the file
file -i raw_data/98-401-X2021006_English_CSV_data_BritishColumbia.csv


# -- This command will:
# -- Attempt to convert from ASCII to UTF-8
# -- Use //TRANSLIT to replace characters that can't be represented in UTF-8
# -- Use -c to skip invalid characters

iconv -f ASCII -t UTF-8//TRANSLIT -c raw_data/98-401-X2021006_English_CSV_data_BritishColumbia.csv > raw_data/98-401-X2021006_English_CSV_data_BritishColumbia-utf8.csv

iconv -f ASCII -t UTF-8//TRANSLIT -c raw_data/98-401-X2021025_English_CSV_data.csv > raw_data/98-401-X2021025_English_CSV_data-utf8.csv

dbt run
dbt test
