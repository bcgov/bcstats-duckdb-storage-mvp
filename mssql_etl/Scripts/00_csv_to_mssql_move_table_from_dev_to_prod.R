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
# After we have table in Dev database: MS SQL server/duckdb,
# we can move the table to Prod database: MS SQL server
# Prod database: MS SQL server
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
library(nanoarrow) # For Arrow integration
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
decimal_conn <- dbConnect(
  odbc::odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)

# optional
# dev database
# duckdb_path <-  file.path(lan_csv_file_path, "DATABASE/Citrix/DEV_Duckdb/dev_duckdb.db")
# duckdb_path <-  ":memory:"
# duckdb_conn <-
#   dbConnect(duckdb::duckdb(dbdir = duckdb_path))

log_dir = "DATABASE/Citrix/log/"

dir.create(file.path(lan_csv_file_path, log_dir))

log_file_path = file.path(
  file.path(lan_csv_file_path, log_dir),
  glue::glue("Read_csv_file_write_to_sqlserver_{Sys.Date()}.log")
)

file_logger = logger(appenders = file_appender(log_file_path))

info(file_logger, "Starte reading csv file write to sqlserver")


############################################################################################################################################
# Health monthly client data
############################################################################################################################################

health_csv_file_list = read_csv(
  file.path(
    lan_csv_file_path,
    "Population Estimates/Sub-Provincial (Annual)/01_Health Monthly Client Data/",
    "clr_ext_csv_files_list_with_sql_names.csv"
  )
)

############################################################################################
csv_col_types_before_202308 = c(
  STUDY_ID = "VARCHAR",
  BIRTH_YR_MON = "VARCHAR",
  SEX = "VARCHAR",
  POSTAL_CODE = "VARCHAR",
  CITY = "VARCHAR",
  STREET_LINE = "VARCHAR",
  LHA = "VARCHAR"
)

# row names should be converted to a column
csv_col_types_before_202308_df <- csv_col_types_before_202308 %>%
  as.data.frame() %>%
  tibble::rownames_to_column("COLUMN_NAME") %>%
  set_names(c("COLUMN_NAME", "COLUMN_TYPE"))
############################################################################################
csv_col_types = c(
  STUDY_ID = "VARCHAR",
  BIRTH_YR_MON = "VARCHAR",
  SEX = "VARCHAR",
  POSTAL_CODE = "VARCHAR",
  CITY = "VARCHAR",
  STREET_LINE = "VARCHAR",
  LHA = "VARCHAR",
  CHSA = "VARCHAR",
  LATITUDE = "DOUBLE",
  LONGITUDE = "DOUBLE",
  EFF_DATE = "DATE",
  END_DATE = "DATE"
)

csv_col_types_df <- csv_col_types %>%
  as.data.frame() %>%
  tibble::rownames_to_column("COLUMN_NAME") %>%
  set_names(c("COLUMN_NAME", "COLUMN_TYPE"))


health_csv_file_list$sql_table_name
############################################################################################
# copy health tables from dev to prod in mssql
for (i in 1:length(health_csv_file_list$sql_table_name)) {
  sql_table_name = health_csv_file_list$sql_table_name[i]
  log_info(sprintf("Start processing table '%s'.", sql_table_name))

  # add the date part back to the new table name
  new_sql_table_name <- get_new_table_name(sql_table_name)

  if (sql_table_name == "CLR_EXT_20190527") {
    # do nothing
  } else if (i < 75) {
    # file before 20230828
    csv_col_types_before_202308_df %>%
      pull(COLUMN_NAME) %>%
      paste0(collapse = ",") -> col_to_copy
  } else {
    csv_col_types_df %>%
      pull(COLUMN_NAME) %>%
      paste0(collapse = ",") -> col_to_copy
  }
  dbExistsTable(
    conn = decimal_conn,
    name = Id(schema = "dev", table = sql_table_name)
  ) -> dev_table_exists
  if (dev_table_exists) {
    log_info(sprintf("The table '%s' exists in dev.", sql_table_name))
    copy_sql_query = glue::glue(
      "IF OBJECT_ID('prod.{sql_table_name}', 'U') IS NOT NULL
      BEGIN
        DROP TABLE prod.{sql_table_name}
      END
      SELECT {col_to_copy} INTO prod.{new_sql_table_name} FROM dev.{sql_table_name}"
    )
    dbExecute(decimal_conn, copy_sql_query)
  }
  # copy table from dev to prod

  # make sure the table is copied to in prod database
  dbExistsTable(
    conn = decimal_conn,
    name = Id(schema = "prod", table = new_sql_table_name)
  ) -> prod_table_exists
  # then drop tables from dev database
  if (prod_table_exists) {
    log_info(sprintf("The table '%s' exists in prod.", new_sql_table_name))
    drop_sql_query = glue::glue(
      "IF OBJECT_ID('dev.{sql_table_name}', 'U') IS NOT NULL
        BEGIN
          DROP TABLE dev.{sql_table_name}
        END"
    )
    dbExecute(decimal_conn, drop_sql_query)
  }
}

############################################################################################
# other tables

# copy tables from dev to prod in mssql
# list tables in dev schema
dev_table_list = dbListTables(decimal_conn, schema_name = "dev")


for (i in 1:length(dev_table_list)) {
  sql_table_name = dev_table_list[i]
  log_info(sprintf("Start processing table '%s'.", sql_table_name))

  # only if table name contains "bca_folio" we create new name, otherwise keep the same name
  if (str_detect(sql_table_name, "bca_folio")) {
    # remove ""02_WEEKLY" from the table name
    new_sql_table_name <- str_remove_all(sql_table_name, "02_WEEKLY")
    # add prefix "FCT" to the table name
    new_sql_table_name <- paste0("FCT_", new_sql_table_name)
  } else {
    new_sql_table_name <- sql_table_name
  }

  dbExistsTable(
    conn = decimal_conn,
    name = Id(schema = "dev", table = sql_table_name)
  ) -> dev_table_exists
  if (dev_table_exists) {
    copy_sql_query = glue::glue(
      "IF OBJECT_ID('prod.{sql_table_name}', 'U') IS NOT NULL
      BEGIN
        DROP TABLE prod.{sql_table_name}
      END
      SELECT * INTO prod.{new_sql_table_name} FROM dev.{sql_table_name}"
    )
    dbExecute(decimal_conn, copy_sql_query)
  }
  # copy table from dev to prod

  # make sure the table is copied to in prod database
  dbExistsTable(
    conn = decimal_conn,
    name = Id(schema = "prod", table = new_sql_table_name)
  ) -> prod_table_exists
  # then drop tables from dev database
  if (prod_table_exists) {
    drop_sql_query = glue::glue(
      "IF OBJECT_ID('dev.{sql_table_name}', 'U') IS NOT NULL
        BEGIN
          DROP TABLE dev.{sql_table_name}
        END"
    )
    dbExecute(decimal_conn, drop_sql_query)
  }
}
