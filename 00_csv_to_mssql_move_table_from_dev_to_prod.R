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
health_table_list = c(
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
  # 'CLR_EXT_20210609',
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
  'CLR_EXT_20221028',
  'CLR_EXT_20221129',
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
  'BC_STAT_POPULATION_ESTIMATES_20240926'
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

############################################################################################
# copy tables from dev to prod in mssql
for (i in 1:length(health_table_list)) {
  sql_table_name = health_table_list[i]
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
