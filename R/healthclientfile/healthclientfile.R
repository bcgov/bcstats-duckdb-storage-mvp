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

# Copyright 2025 Province of British Columbia
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


############################################################################################################################################
# Dev database: duckdb, Prod database: MS SQL server
############################################################################################################################################


# Load required library
library(tidyverse)
library(fs)
library(stringr)
library(DBI)
library(odbc)
library(dplyr)
library(arrow)
# install.packages("\\\\Client\\C$\\Users\\YourUserName\\Downloads\\nanoarrow_0.6.0.tar.gz", repos = NULL, type = "source")
library(nanoarrow)  # For Arrow integration
library(duckdb)
library(log4r)
source("./R/functions.r")



# ---- Configuration ----
# prod database
db_config <- config::get("decimal")
my_schema <- config::get("myschema")
# ---- Connection to decimal ----
decimal_conn <- dbConnect(odbc::odbc(),
                          Driver = db_config$driver,
                          Server = db_config$server,
                          Database = db_config$database,
                          Trusted_Connection = "True")

# Query to list all tables in the DuckDB database
dev_tables <- dbGetQuery(decimal_conn, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'dev';")

# Display the list of tables
print(dev_tables)

health_client_tables <- dev_tables %>% filter(str_detect(table_name, pattern = "CLR_EXT_|bc_stat_population_estimates|BC_STAT_POPULATION_ESTIMATES"))
health_client_tables$table_name %>% paste(  collapse = "', '")

table_names = c(
  'CLR_EXT_20200213_for_201107',
  'CLR_EXT_20160927',
  'CLR_EXT_20161027',
  'CLR_EXT_20161128',
  'CLR_EXT_20161229',
  'CLR_EXT_20170126',
  'CLR_EXT_20170227',
  'CLR_EXT_20170324',
  'CLR_EXT_20170427',
  'CLR_EXT_20170626',
  'CLR_EXT_20170727',
  'CLR_EXT_20170825',
  'CLR_EXT_20170928',
  'CLR_EXT_20171026',
  'CLR_EXT_20171128',
  'CLR_EXT_20171228',
  'CLR_EXT_20180228',
  'CLR_EXT_20180327',
  'CLR_EXT_20180426',
  'CLR_EXT_20180529',
  'CLR_EXT_20180628',
  'CLR_EXT_20180829',
  'CLR_EXT_20181129',
  'CLR_EXT_20181224',
  'CLR_EXT_20190128',
  'CLR_EXT_20190228',
  'CLR_EXT_20190327',
  'CLR_EXT_20190429',
  'CLR_EXT_20190628',
  'CLR_EXT_20190729',
  'CLR_EXT_20190829',
  'CLR_EXT_20180730',
  'CLR_EXT_20190927',
  'CLR_EXT_20191028',
  'CLR_EXT_20191128',
  'CLR_EXT_20191224',
  'CLR_EXT_20200127',
  'CLR_EXT_20200227',
  'CLR_EXT_20200330',
  'CLR_EXT_20200427',
  'CLR_EXT_20200525',
  'CLR_EXT_20200629',
  'CLR_EXT_20200729',
  'CLR_EXT_20200825',
  'CLR_EXT_20201130',
  'CLR_EXT_20201229',
  'CLR_EXT_20210128',
  'CLR_EXT_20210225',
  'CLR_EXT_20210430',
  'CLR_EXT_20210609',
  'CLR_EXT_20210628',
  'CLR_EXT_20210729',
  'CLR_EXT_20210830',
  'CLR_EXT_20210927',
  'CLR_EXT_20211028',
  'CLR_EXT_20211129',
  'CLR_EXT_20211230',
  'CLR_EXT_20220128',
  'CLR_EXT_20220228',
  'CLR_EXT_20220330',
  'CLR_EXT_20220430',
  'CLR_EXT_20220530',
  'CLR_EXT_20220627',
  'CLR_EXT_20220728',
  'CLR_EXT_20220829',
  'CLR_EXT_20220927',
  'CLR_EXT_202201028',
  'CLR_EXT_202201129',
  'CLR_EXT_20230103',
  'CLR_EXT_20230227',
  'BC_STAT_POPULATION_ESTIMATES_20230327',
  'CLR_EXT_20230426',
  'CLR_EXT_20230525',
  'CLR_EXT_20230626',
  'CLR_EXT_20230726',
  'CLR_EXT_20230828',
  'CLR_EXT_20230927',
  'CLR_EXT_20231030',
  'CLR_EXT_20231127',
  'CLR_EXT_20231227',
  'BC_STAT_POPULATION_ESTIMATES_20240129',
  'BC_STAT_POPULATION_ESTIMATES_20240226',
  'BC_STAT_POPULATION_ESTIMATES_20240326',
  'BC_STAT_POPULATION_ESTIMATES_20240429',
  'BC_STAT_POPULATION_ESTIMATES_20240527',
  'BC_STAT_POPULATION_ESTIMATES_20240628',
  'BC_STAT_POPULATION_ESTIMATES_20240726',
  'BC_STAT_POPULATION_ESTIMATES_20240827',
  'bc_stat_population_estimates_20240926'
)

# Function to extract date parts from table name
extract_date_parts <- function(table_name) {
  date_part <- gsub(".*_([0-9]{8}).*", "\\1", table_name)
  year <- substr(date_part, 1, 4)
  month <- substr(date_part, 5, 6)
  day <- substr(date_part, 7, 8)
  list(year = year, month = month, day = day)
}

# Helper function to get the last day of a month
get_last_day <- function(year, month) {
  # Convert to numeric to handle edge cases
  month <- as.numeric(month)
  year <- as.numeric(year)

  # Handle December (month 12)
  if (month == 12) {
    next_month <- as.Date(paste0(year + 1, "-01-01")) # January of the next year
  } else {
    next_month <- as.Date(paste0(year, "-", sprintf("%02d", month + 1), "-01"))
  }

  last_day <- next_month - 1
  format(last_day, "%d")
}

# Generate SQL query
sql_query <- "SELECT * FROM (\n"


for (i in seq_along(table_names)) {
  table <- table_names[i]
  # print(table)
  date_parts <- extract_date_parts(table)

  # Determine if the date is before 202308
  table_date <- as.numeric(paste0(date_parts$year, date_parts$month))


  # Calculate the previous month and year
  previous_month <- as.numeric(date_parts$month) - 1
  previous_year <- as.numeric(date_parts$year)
  if (previous_month == 0) {
    previous_month <- 12
    previous_year <- previous_year - 1
  }

  # Format previous month and year
  previous_month_str <- sprintf("%02d", previous_month)
  previous_year_str <- as.character(previous_year)

  # Generate default dates for NULL handling
  eff_date_default <- paste0(previous_year_str, "-", previous_month_str, "-01")
  end_date_default <- paste0(previous_year_str, "-", previous_month_str, "-", get_last_day(previous_year_str, previous_month_str))



  # is_null_condition <- if (table_date < 202308) {
  #   "NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE]"
  # } else {
  #   "[CHSA], [LATITUDE], [LONGITUDE]"
  # }


  is_null_condition <- if (table_date < 202308) {

    paste0(" NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
             '", eff_date_default, "' AS [EFF_DATE],
             '", end_date_default, "' AS [END_DATE]")

  } else {
    paste0(" [CHSA], [LATITUDE], [LONGITUDE],
             ISNULL([EFF_DATE], '", eff_date_default, "') AS [EFF_DATE],
             ISNULL([END_DATE], '", end_date_default, "') AS [END_DATE]")
  }

  sql_query <- paste0(
    sql_query,
    "    SELECT TOP (1000) '", date_parts$year, "' AS effective_year, '",
    date_parts$month, "' AS effective_month, '",
    date_parts$day, "' AS effective_day, \n",
    "           [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE], \n",
    "           [LHA], ", is_null_condition, "\n",
    # "           ISNULL([EFF_DATE], '", eff_date_default, "') AS [EFF_DATE], \n",
    # "           ISNULL([END_DATE], '", end_date_default, "') AS [END_DATE]\n",
    "    FROM dev.", table
  )

  if (i < length(table_names)) {
    sql_query <- paste0(sql_query, "\n UNION ALL\n")
  } else {
    sql_query <- paste0(sql_query, "\n")
  }
}


sql_query <- paste0(sql_query, ") AS CombinedTables;")

# Print the SQL query
cat(sql_query)

sql_query %>% write_lines("./R/healthclientfile/union_all_health_client_file.sql")

