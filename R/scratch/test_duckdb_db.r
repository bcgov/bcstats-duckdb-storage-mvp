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

if(!require(pacman)){
  install.packages("pacman")
}

pacman::p_load(odbc, tidyverse, config, DBI, dbplyr,nanoarrow, arrow, duckdb,tictoc)

# import functions from utils.r
source(file = "./r-implementation/utils/utils.r")

# Define the path to the test CSV folder
# This path is retrieved from the configuration file
test_csv_folder = config::get("test_sql_server_csv")


# Define the folder path
db_folder_path <- file.path(test_csv_folder, "db")

dev_db_path <- file.path(db_folder_path, "bcstats_db_dev.duckdb")

dev_db_con <- duckdb::dbConnect(duckdb::duckdb(), dbdir = dev_db_path)

# Query to list all tables in the DuckDB database
tables <- dbGetQuery(dev_db_con, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';")

# Display the list of tables
print(tables)

# Disconnect from the database
dbDisconnect(dev_db_con, shutdown = TRUE)
