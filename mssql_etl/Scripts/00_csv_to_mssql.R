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
library(glue)
library(readr)
# library(safepaths)
# install.packages("\\\\Client\\C$\\Users\\YourUserName\\Downloads\\archive_1.1.11.tar.gz", repos = NULL, type = "source")
library(archive)
source("./mssql_etl/Scripts/functions.r")
# source("./R/functions.r")

# This path is retrieved from the configuration file
lan_csv_file_path = config::get("lan_csv_file_path")



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

# optional
# dev database
# duckdb_path <-  file.path(lan_csv_file_path, "DATABASE/Citrix/DEV_Duckdb/dev_duckdb.db")
# duckdb_path <-  ":memory:"
# duckdb_conn <-
#   dbConnect(duckdb::duckdb(dbdir = duckdb_path))



log_dir= "DATABASE/Citrix/log/"

dir.create(file.path(lan_csv_file_path,log_dir))

log_file_path = file.path(file.path(lan_csv_file_path,log_dir), glue::glue("Read_csv_file_write_to_sqlserver_{Sys.Date()}.log"))

file_logger = logger(appenders = file_appender(log_file_path))

info(file_logger, "Starte reading csv file write to sqlserver")


############################################################################################################################################
# Health monthly client data
############################################################################################################################################

# Read table name and CSV file path and if already created, and if need to be created from an Excel file which should be defined by DBA/project manager.
# table_list = tibble()

health_csv_file_list = read_csv(
  file.path(
    lan_csv_file_path,
    "Population Estimates/Sub-Provincial (Annual)/01_Health Monthly Client Data/",
    "clr_ext_csv_files_list_with_sql_names.csv"
  )
)


# for debug
# health_csv_file_list= health_csv_file_list %>%
#   mutate(  file_loaded = if_else(row_number() %in% 86:88, FALSE, TRUE))
# optional
# recommend the iconv command-line tool to convert files with encodings not supported by read_csv to UTF-8. For example:
# cl_convert = "iconv -f ISO-8859-2 -t UTF-8 input.csv > input-utf-8.csv"

# for buildin duckdb query
# schema_string <- "{
#   'STUDY_ID': 'VARCHAR',
#   'BIRTH_YR_MON': 'INTEGER',
#   'SEX': 'VARCHAR',
#   'POSTAL_CODE': 'VARCHAR',
#   'CITY': 'VARCHAR',
#   'STREET_LINE': 'VARCHAR',
#   'LHA': 'VARCHAR',
#   'CHSA': 'VARCHAR',
#   'LATITUDE': 'DOUBLE',
#   'LONGITUDE': 'DOUBLE',
#   'EFF_DATE': 'DATE',
#   'END_DATE': 'DATE'
# }"


csv_col_types_before_202308 = c(
                    STUDY_ID = "VARCHAR",
                    BIRTH_YR_MON = "INTEGER",
                    SEX = "VARCHAR",
                    POSTAL_CODE = "VARCHAR",
                    CITY = "VARCHAR",
                    STREET_LINE = "VARCHAR",
                    LHA = "VARCHAR")

csv_col_types = c(  STUDY_ID = "VARCHAR",
                BIRTH_YR_MON = "INTEGER",
                SEX = "VARCHAR",
                POSTAL_CODE = "VARCHAR",
                CITY = "VARCHAR",
                STREET_LINE = "VARCHAR",
                LHA = "VARCHAR",
                CHSA = "VARCHAR",
                LATITUDE = "DOUBLE",
                LONGITUDE = "DOUBLE",
                EFF_DATE = "DATE",
                END_DATE = "DATE")

for (i in 1:nrow(health_csv_file_list)) {
  one_table = health_csv_file_list %>% slice(i)

  sql_table_name = one_table %>%
    pull(sql_table_name)
  file_name = one_table %>%
    pull(file_name)
  subfolder_path  = one_table %>%
    pull(subfolder_path)
  log_info(sprintf("Start processing table '%s'.", file_name))

  if (one_table$file_loaded | sql_table_name == "CLR_EXT_20190527") {
    # do nothing
  } else if (i < 86) {
   # file before 20230828
    copy_duckdb_csv_to_mssql(
      csv_path = file.path(subfolder_path, file_name),
      mssql_conn = decimal_conn,
      table_name = sql_table_name,
      target_schema = "dev",
      batch_size = 100*256,
      col_type = csv_col_types_before_202308
    )
    health_csv_file_list[i, "file_loaded"] = T

  }
  else {


    copy_duckdb_csv_to_mssql(
      csv_path = file.path(subfolder_path, file_name),
      mssql_conn = decimal_conn,
      table_name = sql_table_name,
      target_schema = "dev",
      batch_size = 100*256,
      col_type = csv_col_types
    )
    health_csv_file_list[i, "file_loaded"] = T
  }

  # save the meta file back to LAN
  health_csv_file_list  %>%  write_csv(
    file.path(
      lan_csv_file_path,
      "Population Estimates/Sub-Provincial (Annual)/01_Health Monthly Client Data/",
      "clr_ext_csv_files_list_with_sql_names.csv"
    )
  )
}



############################################################################################################################################
# bca folio address, sales, description and value files
############################################################################################################################################
# bca folio address, sales, description and value files are inside a zip file, so we need a different function to load them before we use csv_to_mssql.
info(file_logger, "Start reading bc assessment file and writing to sqlserver")

library(archive)
library(readr)
# Load the tools package
library(tools)
# Create a vector containing the keywords you want to match in the CSV filenames:
csv_path_key_list <- c("addresses", "descriptions", "sales", "gnrl_property_values")
csv_path_key_list <- paste0("bca_folio_", csv_path_key_list)

# Path to the main ZIP file
# Specify the path to your ZIP file
main_zip_path <- file.path(lan_csv_file_path, "DATABASE/Citrix/BC Assessment/2025/Feb 2025/BCGW_02001F02_1738965481026_18532.zip")

# List contents of the main ZIP file
main_contents <- archive(main_zip_path)

# list all zip files that we need to load
selected_files <- main_contents[grepl(paste(csv_path_key_list, collapse = "|"), main_contents$path) , ]
selected_files %>% print()
# nested_zip_path = selected_files$path[1]

source("./mssql_etl/Scripts/functions.r")
target_schema = "dev"
mssql_conn = decimal_conn
# i = 1
# loop through the selected_files and load them to mssql
for (i in 2:nrow(selected_files)) {

    nested_zip_path = selected_files %>% slice(i) %>%
    pull(path)

  nested_zip_name = nested_zip_path %>%
    basename()

  log_info(sprintf("Start processing zip file '%s'.", nested_zip_name))
  csv_files_path <- get_csv_file_name(main_zip_path, nested_zip_path)

  log_info(sprintf("Get table name from '%s'.", nested_zip_name))
  csv_data = read_csv_from_nested_zip(main_zip_path, nested_zip_path, csv_files_path)

  # Convert columns: force specific columns to numeric
  numeric_cols <- c("Price", "Value")  # your explicit numeric columns
  csv_data <- convert_column_types(csv_data, numeric_columns = numeric_cols)

  log_info(sprintf("Read table from '%s'.", nested_zip_name))

  # Remove the file extension
  table_name <- file_path_sans_ext(nested_zip_name)


  log_info(sprintf("Start processing table '%s'.", table_name))

  # Connect to DuckDB (in-memory)
  duckdb_conn <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")

  log_info(sprintf("Reading CSV table '%s' into DuckDB.", table_name))

  # Load the data from R into DuckDB
  dbWriteTable(duckdb_conn, table_name, csv_data, overwrite = TRUE)
  # arrow_csv_data <- arrow::arrow_table(csv_data)
  # arrow::to_duckdb(arrow_csv_data, table_name , con = duckdb_conn)
  # Fetch total row count from DuckDB
  total_rows <- get_total_row_count(duckdb_conn, table_name)

  # Check if the table exists in MS SQL Server and drop it if necessary
  check_and_drop_table(mssql_conn, table_name, target_schema)

  # Create table with the custom schema.
  create_custom_table(mssql_conn, table_name, target_schema, csv_data)

  # Verify column types in DuckDB and log schema
  verify_duckdb_schema(duckdb_conn, table_name)

  log_info(sprintf("Started copying table '%s' to MS SQL Server.", table_name))

  copy_data_duckdb_mssql_in_chunk(duckdb_conn, mssql_conn, table_name, target_schema, total_rows )

  log_info(sprintf("Finished copying table '%s' to MS SQL Server.", table_name))

  dbDisconnect(duckdb_conn, shutdown = T)

}


# Disconnect MS SQL Server
dbDisconnect(decimal_conn, shutdown = T)
