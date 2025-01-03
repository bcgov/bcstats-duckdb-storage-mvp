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

# This path is retrieved from the configuration file
lan_csv_file_path = config::get("lan_csv_file_path")

# create a local temp duckdb file
# duckdb_conn <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
# csv_file_path_1 = file.path(lan_csv_file_path,
#                             "Population Estimates/Sub-Provincial (Annual)/01_Health Monthly Client Data/2016_08 (Aug)/CLR_EXT_20160927.csv")
# table_name_1 = "CLR_EXT_20160927"
# #
# test_csv = duckdb_read_csv(conn = duckdb_conn,
#                            name = table_name_1,
#                            files = csv_file_path_1
#                             )
#
# batch_query <- sprintf("SELECT count(*) AS ROW_NUM FROM %s LIMIT %d OFFSET %d", table_name_1, 100, 0)
# arrow_batch <- dbGetQuery(duckdb_conn, batch_query)
# str(arrow_batch)
# arrow_batch$get_next()
# arrow_batch$ROW_NUM
#
# duckdb_table <- tbl(duckdb_conn, "CLR_EXT_20160927")
#
# str(duckdb_table)
#
#
#
# duckdb_table %>% glimpse()


# ---- Configuration ----
db_config <- config::get("decimal")
my_schema <- config::get("myschema")
# ---- Connection to decimal ----
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")


# Example Usage
# duckdb_conn <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
# csv_file_path_1 = file.path(lan_csv_file_path,
#                             "Population Estimates/Sub-Provincial (Annual)/01_Health Monthly Client Data/2016_08 (Aug)/CLR_EXT_20160927.csv")
# table_name_1 = "CLR_EXT_20160927"
#
# copy_csv_to_mssql(
#   csv_path = csv_file_path_1,   # Path to the CSV file
#   mssql_conn = decimal_con,
#   table_name = table_name_1,          # Target table name in MS SQL Server
#   target_schema = "dev",                # Target schema in MS SQL Server
#   batch_size = 100000                    # Batch size
# )
#



log_dir= "DATABASE/Citrix/log/"

dir.create(file.path(lan_csv_file_path,log_dir))

log_file_path = file.path(file.path(lan_csv_file_path,log_dir), glue::glue("Read_csv_file_write_to_sqlserver_{Sys.Date()}.log"))

file_logger = logger(appenders = file_appender(log_file_path))

info(file_logger, "Starte reading csv file write to sqlserver")



# Read table name and CSV file path and if already created, and if need to be created from an Excel file which should be defined by DBA/project manager.
# table_list = tibble()

health_csv_file_list = read_csv(
  file.path(
    lan_csv_file_path,
    "Population Estimates/Sub-Provincial (Annual)/01_Health Monthly Client Data/",
    "clr_ext_csv_files_list_with_sql_names.csv"
  )
)




for (i in 1:nrow(health_csv_file_list)) {
  one_table = health_csv_file_list %>% slice(i)

  sql_table_name = one_table %>%
    pull(sql_table_name)
  file_name = one_table %>%
    pull(file_name)
  subfolder_path  = one_table %>%
    pull(subfolder_path)
  log_info(sprintf("Start processing table '%s'.", file_name))

  if (one_table$file_loaded || sql_table_name == "CLR_EXT_20190527") {
    # do nothing
  } else {


    copy_duckdb_csv_to_mssql(
      csv_path = file.path(subfolder_path, file_name),
      mssql_conn = decimal_con,
      table_name = sql_table_name,
      target_schema = "dev",
      batch_size = 100*256
    )
    health_csv_file_list[i, "file_loaded"] = T
  }

}

health_csv_file_list  %>%  write_csv(
  file.path(
    lan_csv_file_path,
    "Population Estimates/Sub-Provincial (Annual)/01_Health Monthly Client Data/",
    "clr_ext_csv_files_list_with_sql_names.csv"
  )
)



# Disconnect MS SQL Server
dbDisconnect(decimal_con)
